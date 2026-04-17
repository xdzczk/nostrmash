package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// MarkMeilisearchDirty replaces the in-bundle SyncMeilisearch call with a
// cheap upsert into pending_meilisearch_syncs for events that need search
// indexing.
//
// The actual HTTP sync (which can take up to ~30s when Meilisearch is
// slow, single-handedly capping live-pool throughput at
// live_concurrency * 2/min) runs out-of-band in
// DrainPendingMeilisearchSyncBatch, scheduled by the worker's
// meilisearch sweeper loop. Marking is bounded (kind=0 / kind=1 only),
// duplicates collapse via the PRIMARY KEY, and the sweeper can drain
// batches with parallel sync calls so search index lag stays small even
// during heavy ingest.
//
// Skips events whose source row no longer exists (e.g., deleted between
// enqueue and dispatch) and events that aren't kind=0/kind=1; the
// bundle should not dead-letter on this.
func (h *Handlers) MarkMeilisearchDirty(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	if h.meili == nil || !h.meili.Enabled() {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var kind int
	if err := h.pool.QueryRow(ctx, `
		SELECT kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load event for meilisearch dirty marking: %w", err)
	}
	if kind != 0 && kind != 1 {
		return nil
	}

	if _, err := h.pool.Exec(ctx, `
		INSERT INTO pending_meilisearch_syncs (event_id)
		VALUES ($1)
		ON CONFLICT (event_id) DO NOTHING
	`, eventID); err != nil {
		return fmt.Errorf("mark meilisearch dirty: %w", err)
	}
	return nil
}

// DrainPendingMeilisearchSyncBatch claims up to limit pending event_ids
// from pending_meilisearch_syncs using FOR UPDATE SKIP LOCKED, runs
// h.meili.SyncEvent for each, and removes them from the pending table on
// success. Failures re-mark the event so the next cycle retries.
//
// Returns the number of events synced successfully and the first error
// encountered (if any). Callers should log and continue — the next
// cycle will retry any failed events.
func (h *Handlers) DrainPendingMeilisearchSyncBatch(ctx context.Context, limit int) (int, error) {
	if h == nil || h.pool == nil {
		return 0, fmt.Errorf("handlers are not initialized")
	}
	if h.meili == nil || !h.meili.Enabled() {
		return 0, nil
	}
	if limit <= 0 {
		return 0, nil
	}

	eventIDs, err := h.claimPendingMeilisearchSyncs(ctx, limit)
	if err != nil {
		return 0, err
	}
	if len(eventIDs) == 0 {
		return 0, nil
	}

	processed := 0
	var firstErr error
	for _, eventID := range eventIDs {
		// SyncEvent applies its own per-call timeout (syncEventTimeout
		// inside the meili package) so a slow Meilisearch can never
		// permanently wedge the sweeper goroutine.
		if err := h.meili.SyncEvent(ctx, h.pool, eventID); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("meilisearch sync for %s: %w", eventID, err)
			}
			// Re-mark the event so the next sweeper cycle retries it.
			// We already removed it during the claim transaction, so
			// without this re-mark the dirty signal would be lost.
			if _, reinsertErr := h.pool.Exec(ctx, `
				INSERT INTO pending_meilisearch_syncs (event_id)
				VALUES ($1)
				ON CONFLICT (event_id) DO NOTHING
			`, eventID); reinsertErr != nil && firstErr == nil {
				firstErr = fmt.Errorf("re-mark failed event %s: %w", eventID, reinsertErr)
			}
			continue
		}
		processed++
	}
	return processed, firstErr
}

// claimPendingMeilisearchSyncs atomically claims up to limit pending
// event_ids using SELECT ... FOR UPDATE SKIP LOCKED followed by DELETE.
// A crash between the DELETE-commit and the sync commit results in a
// lost sync for the dropped events — but the next event from the same
// kind=0/kind=1 author will re-mark related rows or the periodic
// FullSync can reconcile, so eventual consistency is preserved.
func (h *Handlers) claimPendingMeilisearchSyncs(ctx context.Context, limit int) ([]string, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin pending meilisearch sync claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		WITH claimed AS (
			SELECT event_id
			FROM pending_meilisearch_syncs
			ORDER BY marked_at ASC, event_id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		DELETE FROM pending_meilisearch_syncs p
		USING claimed c
		WHERE p.event_id = c.event_id
		RETURNING p.event_id
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim pending meilisearch syncs: %w", err)
	}
	defer rows.Close()

	eventIDs := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan claimed event id: %w", err)
		}
		eventIDs = append(eventIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read claimed event ids: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit pending meilisearch sync claim: %w", err)
	}
	return eventIDs, nil
}

// PendingMeilisearchSyncBacklog returns the current depth of the dirty
// queue. Exposed for metrics / admin observability.
func (h *Handlers) PendingMeilisearchSyncBacklog(ctx context.Context) (int64, error) {
	if h == nil || h.pool == nil {
		return 0, fmt.Errorf("handlers are not initialized")
	}
	var n int64
	if err := h.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pending_meilisearch_syncs
	`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending meilisearch syncs: %w", err)
	}
	return n, nil
}
