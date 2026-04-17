package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// MarkAuthorAnalyticsDirty replaces the in-bundle ProjectAuthorAnalytics
// call with a cheap upsert into pending_author_analytics_recomputes for
// every pubkey whose author-analytics rows are affected by this event.
//
// The actual heavy aggregation (DELETE + 9-CTE rebuild of
// author_activity_daily plus 5 windowed projections × 3 windows) runs
// out-of-band in DrainPendingAuthorAnalyticsBatch, scheduled by the
// worker's author-analytics sweeper loop. This collapses bursts of N
// events affecting the same pubkey into a single rebuild per sweeper
// cycle instead of N full rebuilds inline, which was the dominant cause
// of derivation-pipeline throughput collapse on hot pubkeys.
//
// Skips events whose source row no longer exists (e.g., deleted between
// enqueue and dispatch); the bundle should not dead-letter on this.
func (h *Handlers) MarkAuthorAnalyticsDirty(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load event for author analytics dirty marking: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	references := deriveEventReferences(eventID, tags)
	pubkeys, err := h.authorAnalyticsAffectedPubkeys(ctx, eventID, kind, pubkey, references, tags)
	if err != nil {
		return err
	}
	if len(pubkeys) == 0 {
		return nil
	}

	// Single round-trip insert. ON CONFLICT (pubkey) DO NOTHING is the right
	// semantics: if the pubkey is already marked dirty, there's nothing more
	// to record — the sweeper will pick it up and recompute, capturing all
	// events that arrived before the recompute starts.
	if _, err := h.pool.Exec(ctx, `
		INSERT INTO pending_author_analytics_recomputes (pubkey)
		SELECT unnest($1::text[])
		ON CONFLICT (pubkey) DO NOTHING
	`, pubkeys); err != nil {
		return fmt.Errorf("mark author analytics dirty: %w", err)
	}
	return nil
}

// DrainPendingAuthorAnalyticsBatch claims up to limit dirty pubkeys from
// pending_author_analytics_recomputes using FOR UPDATE SKIP LOCKED, runs
// the heavy ProjectAuthorAnalytics rebuild for each, and removes them
// from the pending table on success.
//
// The claim uses SKIP LOCKED so multiple sweeper workers can drain the
// table in parallel without blocking on each other. A failure on one
// pubkey leaves it in the table (the deferred rollback covers the
// SELECT...FOR UPDATE), so it will be retried on the next cycle. The
// caller's context controls how long any individual rebuild can run
// before being cancelled.
//
// Returns the number of pubkeys whose rebuild completed successfully and
// the first error encountered (if any). On error, callers should log and
// continue — the next cycle will retry the failed pubkey.
func (h *Handlers) DrainPendingAuthorAnalyticsBatch(ctx context.Context, limit int) (int, error) {
	if h == nil || h.pool == nil {
		return 0, fmt.Errorf("handlers are not initialized")
	}
	if limit <= 0 {
		return 0, nil
	}

	pubkeys, err := h.claimPendingAuthorAnalyticsPubkeys(ctx, limit)
	if err != nil {
		return 0, err
	}
	if len(pubkeys) == 0 {
		return 0, nil
	}

	processed := 0
	var firstErr error
	for _, pubkey := range pubkeys {
		if err := h.projectAuthorAnalyticsForPubkey(ctx, pubkey, nil); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("rebuild author analytics for %s: %w", pubkey, err)
			}
			// Re-mark the pubkey so the next sweeper cycle retries it. We
			// already removed it during the claim transaction, so without
			// this re-mark the dirty signal would be lost.
			if _, reinsertErr := h.pool.Exec(ctx, `
				INSERT INTO pending_author_analytics_recomputes (pubkey)
				VALUES ($1)
				ON CONFLICT (pubkey) DO NOTHING
			`, pubkey); reinsertErr != nil && firstErr == nil {
				firstErr = fmt.Errorf("re-mark failed pubkey %s: %w", pubkey, reinsertErr)
			}
			continue
		}
		processed++
	}
	return processed, firstErr
}

// claimPendingAuthorAnalyticsPubkeys atomically claims up to limit dirty
// pubkeys using SELECT ... FOR UPDATE SKIP LOCKED followed by DELETE. A
// crash between the DELETE-commit and the rebuild commit results in a
// lost rebuild for the dropped pubkeys — but the next event from any of
// them will re-mark them dirty, so the data converges. The window is
// negligible compared to the throughput gain.
func (h *Handlers) claimPendingAuthorAnalyticsPubkeys(ctx context.Context, limit int) ([]string, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin pending author analytics claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		WITH claimed AS (
			SELECT pubkey
			FROM pending_author_analytics_recomputes
			ORDER BY marked_at ASC, pubkey ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		DELETE FROM pending_author_analytics_recomputes p
		USING claimed c
		WHERE p.pubkey = c.pubkey
		RETURNING p.pubkey
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim pending author analytics pubkeys: %w", err)
	}
	defer rows.Close()

	pubkeys := make([]string, 0, limit)
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("scan claimed pubkey: %w", err)
		}
		pubkeys = append(pubkeys, pk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read claimed pubkeys: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit pending author analytics claim: %w", err)
	}
	return pubkeys, nil
}

// PendingAuthorAnalyticsBacklog returns the current depth of the dirty
// queue. Exposed for metrics / admin observability.
func (h *Handlers) PendingAuthorAnalyticsBacklog(ctx context.Context) (int64, error) {
	if h == nil || h.pool == nil {
		return 0, fmt.Errorf("handlers are not initialized")
	}
	var n int64
	if err := h.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pending_author_analytics_recomputes
	`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending author analytics: %w", err)
	}
	return n, nil
}
