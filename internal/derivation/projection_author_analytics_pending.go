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
// On conflict we DO UPDATE SET marked_at = now(). The CAS in
// finalizePendingAuthorAnalyticsClaim relies on marked_at advancing
// during a sweeper rebuild to detect re-marks: a producer that fires
// while the sweeper is mid-rebuild bumps marked_at; phase 3 then
// observes marked_at != captured-at-claim and leaves the row in place
// (clearing only the claim) so the next sweeper cycle re-rebuilds and
// picks up the events the prior rebuild missed. The previous DO NOTHING
// semantics silently dropped re-marks during claims, which could lose
// updates in long quiet periods where no further event for the pubkey
// arrives to re-mark it.
//
// The row-level lock taken by ON CONFLICT DO UPDATE is held only for
// this single-statement transaction (autocommit via pool.Exec), so
// concurrent producers contend for at most a few hundred microseconds
// per upsert. The sweeper-vs-producer row-lock chain that the previous
// "claim+delete in long tx" design produced is gone in this layout
// because the sweeper holds the row lock only for the brief phase-1
// claim transaction.
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

	if _, err := h.pool.Exec(ctx, `
		INSERT INTO pending_author_analytics_recomputes (pubkey)
		SELECT unnest($1::text[])
		ON CONFLICT (pubkey) DO UPDATE SET marked_at = now()
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

// authorAnalyticsClaimLease bounds how long a claim is honored before
// other sweeper goroutines can steal the pubkey back. A worker that
// crashes or hangs mid-rebuild thus holds a pubkey for at most this
// long. Set well above any realistic per-pubkey rebuild duration
// (WORKER_AUTHOR_ANALYTICS_REBUILD_TIMEOUT defaults to 90s); the lease
// is the safety net for whole-process stalls (OOM, deadlock) that
// bypass the per-rebuild timeout.
const authorAnalyticsClaimLease = 5 * time.Minute

// DrainPendingAuthorAnalyticsBatch processes up to limit dirty pubkeys
// from pending_author_analytics_recomputes. Each pubkey moves through
// three short-lived database transactions:
//
//  1. Phase 1 (claim, ~1ms): atomically pick a claimable pubkey using
//     FOR UPDATE SKIP LOCKED filtered by pg_try_advisory_xact_lock,
//     then mark it claimed (claimed_at = now(), claim_token = uuid).
//     Commit immediately, releasing the row lock.
//  2. Phase 2 (rebuild, 1-90s): re-acquire the per-pubkey advisory lock
//     (blocking; uncontested because phase 1 used pg_try_advisory_xact_lock
//     as a claim filter), run the heavy rebuild, commit. Holds NO row
//     lock on the pending table during this phase, so concurrent bundle
//     workers' INSERT ON CONFLICT DO UPDATE on the same pubkey complete
//     in microseconds.
//  3. Phase 3 (cleanup, ~1ms): DELETE WHERE pubkey AND claim_token AND
//     marked_at unchanged. If marked_at advanced (a producer re-marked
//     during the rebuild), instead UPDATE the row to clear the claim so
//     the next sweeper cycle picks it up; the prior rebuild's projection
//     write is still committed but the events that arrived during the
//     rebuild get a follow-up rebuild.
//
// Stale-claim recovery: phase 1 will pick up rows whose claimed_at is
// older than authorAnalyticsClaimLease, so a worker that crashes
// mid-rebuild never permanently parks a pubkey.
//
// The previous in-this-codebase design (atomic claim+lock+rebuild in a
// single long transaction) avoided sweeper-vs-sweeper advisory-lock
// chains but introduced sweeper-vs-producer row-lock chains: the DELETE
// in the long tx held the pending row's lock for the entire 30-160s
// rebuild, blocking every producer trying to mark the same pubkey
// dirty. Production observed dozens of bundle workers stalled on those
// row locks, starving the bundle pool of database connections. The
// 3-phase design eliminates both chains.
//
// Returns the number of pubkeys whose rebuild completed successfully
// and the first error encountered (if any). On error, callers should
// log and continue — the failed pubkey's claim is released, leaving its
// row in pending_author_analytics_recomputes for retry on the next
// cycle.
//
// Equivalent to DrainPendingAuthorAnalyticsBatchWithTimeout(ctx, limit, 0).
func (h *Handlers) DrainPendingAuthorAnalyticsBatch(ctx context.Context, limit int) (int, error) {
	return h.DrainPendingAuthorAnalyticsBatchWithTimeout(ctx, limit, 0)
}

// DrainPendingAuthorAnalyticsBatchWithTimeout is DrainPendingAuthorAnalyticsBatch
// with an additional per-pubkey rebuild timeout. When perPubkeyTimeout > 0
// the phase-2 rebuild is bounded by that duration; on timeout the rebuild
// transaction rolls back, the per-pubkey advisory lock auto-releases, the
// claim is cleared (so the row becomes claimable again immediately, not
// after the lease), and the loop moves on.
//
// This is a safety net: without it, a single hot pubkey with an
// unexpectedly heavy aggregate (e.g., one with hundreds of thousands of
// reactions over 90 days) could hold a pgxpool connection long enough
// to starve bundle workers. The lease alone would cap the damage at 5
// minutes per stuck rebuild; the timeout caps it at the configured
// per-pubkey duration.
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

// processNextPendingAuthorAnalyticsPubkeyWithTimeout runs the 3-phase
// claim/rebuild/cleanup cycle for a single pubkey. When perPubkeyTimeout
// > 0, only the phase-2 rebuild is bounded by that duration; phase 1
// and phase 3 always run on the parent ctx so claim management does not
// race with the timeout. When perPubkeyTimeout is 0, phase 2 uses the
// parent context with no additional deadline.
//
// Returns (false, nil) when there are no claimable pubkeys (queue empty
// or every top-of-queue pubkey is already locked or freshly claimed by
// another goroutine).
func (h *Handlers) processNextPendingAuthorAnalyticsPubkeyWithTimeout(ctx context.Context, perPubkeyTimeout time.Duration) (bool, error) {
	pubkey, claimToken, markedAt, ok, err := h.claimPendingAuthorAnalyticsPubkey(ctx)
	if err != nil || !ok {
		return false, err
	}

	rebuildCtx := ctx
	if perPubkeyTimeout > 0 {
		var cancel context.CancelFunc
		rebuildCtx, cancel = context.WithTimeout(ctx, perPubkeyTimeout)
		defer cancel()
	}

	if rebuildErr := h.rebuildClaimedAuthorAnalyticsPubkey(rebuildCtx, pubkey); rebuildErr != nil {
		// Release the claim so the next sweeper cycle can retry without
		// waiting out the lease. Use a fresh background context bounded
		// by 5 seconds: the parent ctx may already be canceled (which is
		// what caused the rebuild failure in the first place), but the
		// claim-release is local bookkeeping and must still happen.
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = h.releasePendingAuthorAnalyticsClaim(releaseCtx, pubkey, claimToken)
		releaseCancel()
		return false, fmt.Errorf("rebuild author analytics for %s: %w", pubkey, rebuildErr)
	}

	if err := h.finalizePendingAuthorAnalyticsClaim(ctx, pubkey, claimToken, markedAt); err != nil {
		// Rebuild succeeded; finalization is best-effort. Returning the
		// error lets the caller log it; the claim will be reaped via the
		// 5-minute lease.
		return true, fmt.Errorf("finalize author analytics claim for %s: %w", pubkey, err)
	}
	return true, nil
}

// claimPendingAuthorAnalyticsPubkey runs the phase-1 claim transaction.
// Returns (pubkey, claim_token, marked_at, true, nil) on a successful
// claim or ("", "", zero, false, nil) when no pubkey is currently
// claimable.
//
// The query uses a 3-stage CTE:
//   - candidates: top-N rows with no live claim, locked at the row level
//     (FOR UPDATE SKIP LOCKED so concurrent claimers naturally pick
//     different rows).
//   - locked: of those, the first one whose per-pubkey advisory lock is
//     immediately acquirable. The advisory lock is xact-scoped (released
//     at this transaction's commit), so it is purely a claim filter and
//     does not survive into phase 2.
//   - the outer UPDATE: stamp claimed_at and claim_token on the chosen
//     row. The RETURNING clause yields the captured marked_at so phase 3
//     can CAS on it.
func (h *Handlers) claimPendingAuthorAnalyticsPubkey(ctx context.Context) (string, string, time.Time, bool, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return "", "", time.Time{}, false, fmt.Errorf("begin author analytics claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var pubkey, claimToken string
	var markedAt time.Time
	err = tx.QueryRow(ctx, `
		WITH candidates AS (
			SELECT pubkey, marked_at
			FROM pending_author_analytics_recomputes
			WHERE claimed_at IS NULL
			   OR claimed_at < now() - $3::interval
			ORDER BY marked_at ASC, pubkey ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		),
		locked AS (
			SELECT pubkey, marked_at
			FROM candidates
			WHERE pg_try_advisory_xact_lock(
				hashtextextended(pubkey, 0) # hashtextextended($2, 1)
			)
			LIMIT 1
		)
		UPDATE pending_author_analytics_recomputes p
		SET claimed_at = now(),
		    claim_token = gen_random_uuid()
		FROM locked l
		WHERE p.pubkey = l.pubkey
		RETURNING p.pubkey, p.claim_token::text, l.marked_at
	`,
		authorAnalyticsClaimCandidateWindow,
		pubkeyLockNamespaceAuthorAnalytics,
		authorAnalyticsClaimLease,
	).Scan(&pubkey, &claimToken, &markedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", time.Time{}, false, nil
		}
		return "", "", time.Time{}, false, fmt.Errorf("claim pending author analytics pubkey: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", "", time.Time{}, false, fmt.Errorf("commit author analytics claim tx: %w", err)
	}
	return pubkey, claimToken, markedAt, true, nil
}

// rebuildClaimedAuthorAnalyticsPubkey runs the phase-2 rebuild
// transaction. The per-pubkey advisory lock is acquired (blocking) by
// projectAuthorAnalyticsForPubkeyTx via lockPubkeyForWriteTx. Because
// phase 1's claim filter ensures no other sweeper goroutine holds the
// lock, this acquire is essentially uncontested in practice; on the
// rare lease-expiry race it will block briefly for the other goroutine
// to release.
func (h *Handlers) rebuildClaimedAuthorAnalyticsPubkey(ctx context.Context, pubkey string) error {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin author analytics rebuild tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := h.projectAuthorAnalyticsForPubkeyTx(ctx, tx, pubkey, nil, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// finalizePendingAuthorAnalyticsClaim runs the phase-3 cleanup. The
// DELETE is conditional on (claim_token unchanged) AND (marked_at
// unchanged): if either differs, the row was either re-claimed by
// another goroutine after a lease expiry (token differs) or re-marked
// dirty by a producer during the rebuild (marked_at advanced). In
// both cases we leave the row alone or just clear the claim so the
// next sweeper cycle handles it.
func (h *Handlers) finalizePendingAuthorAnalyticsClaim(ctx context.Context, pubkey, claimToken string, markedAt time.Time) error {
	tag, err := h.pool.Exec(ctx, `
		DELETE FROM pending_author_analytics_recomputes
		WHERE pubkey = $1
		  AND claim_token::text = $2
		  AND marked_at = $3
	`, pubkey, claimToken, markedAt)
	if err != nil {
		return fmt.Errorf("finalize delete: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// 0 rows affected: either marked_at advanced or token doesn't match.
	// Clear our claim so the row is immediately claimable. If the token
	// no longer matches (lease-expiry takeover), the UPDATE matches 0
	// rows and is a harmless no-op.
	if _, err := h.pool.Exec(ctx, `
		UPDATE pending_author_analytics_recomputes
		SET claimed_at = NULL,
		    claim_token = NULL
		WHERE pubkey = $1 AND claim_token::text = $2
	`, pubkey, claimToken); err != nil {
		return fmt.Errorf("clear claim after no-op finalize: %w", err)
	}
	return nil
}

// releasePendingAuthorAnalyticsClaim is called when the rebuild fails
// and we want the row to be retried immediately rather than waiting
// out the lease. It is a no-op if the claim has already been taken
// over by another goroutine (token mismatch).
func (h *Handlers) releasePendingAuthorAnalyticsClaim(ctx context.Context, pubkey, claimToken string) error {
	if _, err := h.pool.Exec(ctx, `
		UPDATE pending_author_analytics_recomputes
		SET claimed_at = NULL,
		    claim_token = NULL
		WHERE pubkey = $1 AND claim_token::text = $2
	`, pubkey, claimToken); err != nil {
		return fmt.Errorf("release pending author analytics claim: %w", err)
	}
	return nil
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
