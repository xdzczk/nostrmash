package derivation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// reinsertClaimedMeilisearchSyncsTimeout bounds the best-effort
// re-queue of a claimed batch when SyncEventsBatch failed because the
// caller's context was canceled or expired. It uses
// context.Background() so the re-queue still completes during graceful
// shutdown or after a per-batch deadline trip; the timeout protects
// against an unrelated DB stall wedging the sweeper goroutine.
const reinsertClaimedMeilisearchSyncsTimeout = 5 * time.Second

// MarkMeilisearchDirty replaces the in-bundle SyncMeilisearch call with a
// cheap upsert into pending_meilisearch_syncs for events that need search
// indexing.
//
// The actual HTTP sync (which can take up to ~30s when Meilisearch is
// slow, single-handedly capping live-pool throughput at
// live_concurrency * 2/min) runs out-of-band in
// DrainPendingMeilisearchSyncBatch, scheduled by the worker's
// meilisearch sweeper loop. Marking is bounded (kind=0 profiles plus
// kind=1 / kind=30023 notes), duplicates collapse via the PRIMARY KEY,
// and the sweeper can drain batches with parallel sync calls so search
// index lag stays small even during heavy ingest.
//
// Skips events whose source row no longer exists (e.g., deleted between
// enqueue and dispatch) and events that aren't kind=0/kind=1/kind=30023;
// the bundle should not dead-letter on this.
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
	if kind != 0 && kind != 1 && kind != 30023 {
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

	// Fast path: dispatch the whole batch in a single SyncEventsBatch
	// call. This collapses len(eventIDs) × 2 individual Meilisearch
	// tasks (which Meili processes serially per index) into 3 tasks
	// (notes / profiles / search_documents), removing the per-event
	// HTTP + waitForTask round-trip cost that previously capped sweeper
	// throughput regardless of goroutine count.
	batchErr := h.meili.SyncEventsBatch(ctx, h.pool, eventIDs)
	if batchErr == nil {
		return len(eventIDs), nil
	}

	// Distinguish "Meili saturated / context expired" from "data
	// poisoning". The per-event fallback below is designed to isolate
	// a single bad document so it does not poison an otherwise healthy
	// batch — but when Meilisearch itself is overloaded (its
	// per-index task queue is the bottleneck) every per-event call
	// will hit the same saturation and time out. With concurrency=N
	// goroutines that turns into a hard livelock: each cycle deletes
	// 500 rows during claim, then per-event fails for ~all of them and
	// re-inserts ~all of them, so net drain ≈ 0 while production keeps
	// adding work.
	//
	// On context.DeadlineExceeded / context.Canceled, re-insert the
	// whole claimed batch in a single statement and let the next cycle
	// retry it as a batch when Meili recovers. This is correctness-safe
	// because (a) MarkMeilisearchDirty is idempotent on event_id and
	// (b) Meilisearch upserts are idempotent, so a duplicate sync from
	// a race is wasteful but not corrupting.
	if errors.Is(batchErr, context.DeadlineExceeded) || errors.Is(batchErr, context.Canceled) {
		if reErr := h.reinsertClaimedMeilisearchSyncs(eventIDs); reErr != nil {
			slog.Error(
				"meilisearch_sweeper_reinsert_failed",
				"reason", "context_expired",
				"claimed", len(eventIDs),
				"reinsert_error", reErr,
				"batch_error", batchErr,
			)
			return 0, fmt.Errorf("reinsert claimed events after meilisearch timeout (%w): %w", batchErr, reErr)
		}
		slog.Warn(
			"meilisearch_batch_timeout_reinsert",
			"claimed", len(eventIDs),
			"batch_error", batchErr,
		)
		return 0, fmt.Errorf("meilisearch batch sync timed out, re-queued %d events: %w", len(eventIDs), batchErr)
	}

	// Non-timeout error: fall back to per-event sync so good events
	// still drain and bad events get isolated (re-marked individually
	// for the next cycle's retry rather than poisoning the entire
	// batch).
	processed := 0
	var firstErr error
	for _, eventID := range eventIDs {
		if err := h.meili.SyncEvent(ctx, h.pool, eventID); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("meilisearch sync for %s: %w", eventID, err)
			}
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
	if firstErr == nil && processed > 0 {
		// Whole batch recovered via per-event fallback; surface the
		// original batch error so logs/metrics still record that
		// the fast path failed.
		return processed, fmt.Errorf("meilisearch batch sync fell back to per-event: %w", batchErr)
	}
	return processed, firstErr
}

// reinsertClaimedMeilisearchSyncs re-queues a previously-claimed
// (DELETEd) batch using a fresh background context so it succeeds even
// when the caller's context is canceled (graceful shutdown) or already
// past its deadline (per-batch timeout). Idempotent via the PRIMARY KEY
// on event_id.
func (h *Handlers) reinsertClaimedMeilisearchSyncs(eventIDs []string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	if len(eventIDs) == 0 {
		return nil
	}
	bgCtx, cancel := context.WithTimeout(context.Background(), reinsertClaimedMeilisearchSyncsTimeout)
	defer cancel()
	if _, err := h.pool.Exec(bgCtx, `
		INSERT INTO pending_meilisearch_syncs (event_id)
		SELECT unnest($1::text[])
		ON CONFLICT (event_id) DO NOTHING
	`, eventIDs); err != nil {
		return fmt.Errorf("reinsert claimed meilisearch syncs: %w", err)
	}
	return nil
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
