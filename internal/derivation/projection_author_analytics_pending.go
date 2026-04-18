package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// MarkAuthorAnalyticsDirty replaces the in-bundle ProjectAuthorAnalytics
// call with a cheap upsert into pending_author_analytics_recomputes for
// every pubkey whose author-analytics rows are affected by this event.
//
// The actual heavy aggregation (DELETE + multi-CTE rebuild of
// author_activity_daily plus 5 windowed projections × N windows) runs
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

// authorAnalyticsClaimCandidateWindow caps the candidate-row scan inside
// the atomic claim+lock query. The scan is filtered through
// pg_try_advisory_xact_lock as a WHERE clause: with N candidates and K
// goroutines already holding hot-pubkey locks, we pick the first
// candidate whose advisory lock is acquirable. A larger window improves
// the chance any one call returns a usable pubkey when many top-of-queue
// pubkeys are simultaneously hot, at the cost of scanning more rows per
// call. 32 is empirically large enough to keep all goroutines busy on
// production workloads while still bounding per-call scan cost.
const authorAnalyticsClaimCandidateWindow = 32

// DrainPendingAuthorAnalyticsBatch processes up to limit dirty pubkeys
// from pending_author_analytics_recomputes. Each pubkey is claimed and
// rebuilt in a single atomic transaction that holds the per-pubkey
// advisory lock for the entire rebuild duration.
//
// The atomic claim+lock+rebuild approach (vs. the previous "claim all,
// commit, then rebuild each in its own transaction" pattern) eliminates
// the lock-chain pathology observed in production:
//
//   - Old pattern: claim X, commit DELETE, start rebuild tx that locks X.
//     During the gap between the two commits, new events for X re-mark
//     it dirty. Another sweeper goroutine claims X again and queues up
//     behind the advisory lock, blocking for the duration of the
//     rebuild (observed: 145s wait chains for hot pubkeys, with 10
//     goroutines all serialized on the same lock and consuming all
//     pgxpool connections).
//
//   - New pattern: claim X via FOR UPDATE SKIP LOCKED filtered by
//     pg_try_advisory_xact_lock, so a goroutine never picks a pubkey
//     it can't immediately lock. A different goroutine simply picks a
//     different pubkey, naturally distributing work across the dirty
//     queue without contention.
//
// Returns the number of pubkeys whose rebuild completed successfully and
// the first error encountered (if any). On error, callers should log and
// continue — the failed pubkey's transaction rolled back, leaving its
// row in pending_author_analytics_recomputes for retry on the next
// cycle.
//
// Equivalent to DrainPendingAuthorAnalyticsBatchWithTimeout(ctx, limit, 0).
func (h *Handlers) DrainPendingAuthorAnalyticsBatch(ctx context.Context, limit int) (int, error) {
	return h.DrainPendingAuthorAnalyticsBatchWithTimeout(ctx, limit, 0)
}

// DrainPendingAuthorAnalyticsBatchWithTimeout is DrainPendingAuthorAnalyticsBatch
// with an additional per-pubkey rebuild timeout. When perPubkeyTimeout > 0
// each individual rebuild's transaction is bounded by that duration; on
// timeout the transaction rolls back, the per-pubkey advisory lock
// auto-releases, and the pending row is restored — so the pubkey is
// automatically retried on the next sweeper cycle.
//
// This is a safety net: without it, a single hot pubkey with an
// unexpectedly heavy aggregate (e.g., one with hundreds of thousands of
// reactions over 90 days) could hold a pgxpool connection long enough to
// starve bundle workers, reproducing the production stall we just fixed
// architecturally.
func (h *Handlers) DrainPendingAuthorAnalyticsBatchWithTimeout(ctx context.Context, limit int, perPubkeyTimeout time.Duration) (int, error) {
	if h == nil || h.pool == nil {
		return 0, fmt.Errorf("handlers are not initialized")
	}
	if limit <= 0 {
		return 0, nil
	}

	processed := 0
	var firstErr error
	for i := 0; i < limit; i++ {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		ok, err := h.processNextPendingAuthorAnalyticsPubkeyWithTimeout(ctx, perPubkeyTimeout)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !ok {
			break
		}
		processed++
	}
	return processed, firstErr
}

// processNextPendingAuthorAnalyticsPubkey atomically claims one dirty
// pubkey (using row-level SKIP LOCKED + try-advisory-lock as a filter)
// and runs the rebuild within the same transaction. The advisory lock
// is held for the lifetime of the transaction, so two sweeper
// goroutines can never contend on the same hot pubkey: one of them
// holds the lock, the other simply skips it and picks a different
// pubkey from the candidate window.
//
// On rebuild failure, the deferred rollback restores the
// pending_author_analytics_recomputes row, so the pubkey is
// automatically retried on the next sweeper cycle without any explicit
// re-mark.
//
// Returns (false, nil) when there are no claimable pubkeys (queue
// empty, or every top-of-queue pubkey is already locked by another
// goroutine).
func (h *Handlers) processNextPendingAuthorAnalyticsPubkey(ctx context.Context) (bool, error) {
	return h.processNextPendingAuthorAnalyticsPubkeyWithTimeout(ctx, 0)
}

// processNextPendingAuthorAnalyticsPubkeyWithTimeout is the
// rebuild-timeout-aware sibling of processNextPendingAuthorAnalyticsPubkey.
// When perPubkeyTimeout > 0, the entire claim+rebuild transaction is
// bounded by that duration via context.WithTimeout.
func (h *Handlers) processNextPendingAuthorAnalyticsPubkeyWithTimeout(ctx context.Context, perPubkeyTimeout time.Duration) (bool, error) {
	if perPubkeyTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, perPubkeyTimeout)
		defer cancel()
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin pending author analytics processing tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var pubkey string
	err = tx.QueryRow(ctx, `
		WITH candidates AS (
			SELECT pubkey
			FROM pending_author_analytics_recomputes
			ORDER BY marked_at ASC, pubkey ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		),
		locked AS (
			SELECT pubkey
			FROM candidates
			WHERE pg_try_advisory_xact_lock(
				hashtextextended(pubkey, 0) # hashtextextended($2, 1)
			)
			LIMIT 1
		)
		DELETE FROM pending_author_analytics_recomputes p
		USING locked l
		WHERE p.pubkey = l.pubkey
		RETURNING p.pubkey
	`, authorAnalyticsClaimCandidateWindow, pubkeyLockNamespaceAuthorAnalytics).Scan(&pubkey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("claim+lock pending author analytics pubkey: %w", err)
	}

	// projectAuthorAnalyticsForPubkeyTx will call lockPubkeyForWriteTx
	// which re-acquires the same advisory lock. Postgres advisory locks
	// are reentrant within a session/transaction (acquisition count is
	// incremented), so the inner lockPubkeyForWriteTx is a no-op cost
	// and remains in place to preserve correctness for any other call
	// path that invokes projectAuthorAnalyticsForPubkeyTx without
	// pre-claiming.
	if err := h.projectAuthorAnalyticsForPubkeyTx(ctx, tx, pubkey, nil); err != nil {
		return false, fmt.Errorf("rebuild author analytics for %s: %w", pubkey, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit author analytics rebuild for %s: %w", pubkey, err)
	}
	return true, nil
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
