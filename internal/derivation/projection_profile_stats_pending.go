package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// MarkProfileStatsDirty replaces the in-bundle ProjectProfilePublicStats
// and ProjectProfileDiscoveryStats calls with a cheap upsert into
// pending_profile_stats_recomputes for every pubkey whose profile-stats
// rows are affected by this event.
//
// Historically both projections did expensive per-pubkey aggregates on
// hot pubkeys (full COUNT(*) rebuilds for public stats; multi-table
// window scans + unbounded MAX(created_at) UNION for discovery). With
// WORKER_INCREMENTAL_* defaults on, the sweeper skips the public-stats
// rebuild entirely and rolls discovery scores from
// author_hourly_activity / follower_gain_events /
// profile_discovery_recent_activity. The mark-and-sweep path remains so
// bursts still coalesce and the flag-off / rebuild escape hatches keep
// working.
//
// The new design: the bundle does a cheap upsert into
// pending_profile_stats_recomputes for each affected pubkey.
// DrainPendingProfileStatsBatch runs both projections per dirty pubkey
// out-of-band, naturally coalescing bursts (N events affecting the
// same pubkey collapse into a single recompute per sweeper cycle).
//
// On conflict we DO UPDATE SET marked_at = now(). The CAS in
// finalizePendingProfileStatsClaim relies on marked_at advancing
// during a sweeper rebuild to detect re-marks: a producer that fires
// while the sweeper is mid-rebuild bumps marked_at; phase 3 then
// observes marked_at != captured-at-claim and leaves the row in place
// (clearing only the claim) so the next sweeper cycle re-rebuilds.
// The previous DO NOTHING semantics silently dropped re-marks during
// claims, which could lose updates in long quiet periods where no
// further event for the pubkey arrived to re-mark it.
//
// The row-level lock taken by ON CONFLICT DO UPDATE is held only for
// this single-statement transaction (autocommit via pool.Exec), so
// concurrent producers contend for at most a few hundred microseconds
// per upsert. The sweeper-vs-producer row-lock chain that the previous
// "claim+delete in long tx" design produced is gone in this layout
// because the sweeper holds the row lock only for the brief phase-1
// claim transaction.
//
// Affected-pubkey semantics: we use the discovery-stats affected set,
// which is a superset of public-stats (event author plus referenced
// pubkeys for replies / reposts / reactions / zaps / contact-list
// follows). The sweeper recomputes both projections per dirty pubkey;
// for pubkeys that are only discovery-affected (e.g., zap receivers)
// the public_stats recompute will produce an unchanged row, which is
// harmless and the cost is bounded.
//
// Skips events whose source row no longer exists (e.g., deleted
// between enqueue and dispatch); the bundle should not dead-letter
// on this.
func (h *Handlers) MarkProfileStatsDirty(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
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
		return fmt.Errorf("load event for profile stats dirty marking: %w", err)
	}
	// kind=3 contact-list rewrites are already dirty-marked inside
	// ProjectContactListsLatest (author + previous followed + new
	// contacts) in the same derive bundle. Re-marking here would only
	// bump marked_at again — amplifying pending-table UPDATE churn and
	// autovacuum pressure — without expanding the affected set.
	if kind == 3 {
		return nil
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	references := deriveEventReferences(eventID, tags)

	// affectedProfileDiscoveryPubkeysTx requires a tx, so we open a
	// short read-only one. The queries are simple lookups against
	// reply_count_contributions / repost_events / reaction_events /
	// follower_edges by source_event_id, which are cheap and bounded.
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin profile stats dirty-marking tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	pubkeys, err := h.affectedProfileDiscoveryPubkeysTx(ctx, tx, eventID, kind, references, tags)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile stats dirty-marking tx: %w", err)
	}
	if len(pubkeys) == 0 {
		return nil
	}

	if _, err := h.pool.Exec(ctx, `
		INSERT INTO pending_profile_stats_recomputes (pubkey)
		SELECT unnest($1::text[])
		ON CONFLICT (pubkey) DO UPDATE SET marked_at = now()
	`, pubkeys); err != nil {
		return fmt.Errorf("mark profile stats dirty: %w", err)
	}
	return nil
}

// profileStatsClaimCandidateWindow caps the candidate-row scan inside
// the phase-1 claim query. See author_analytics' equivalent constant
// for rationale.
const profileStatsClaimCandidateWindow = 32

// profileStatsClaimLease bounds how long a claim is honored before
// other sweeper goroutines can steal the pubkey back. See
// authorAnalyticsClaimLease for rationale.
const profileStatsClaimLease = 5 * time.Minute

// DrainPendingProfileStatsBatch processes up to limit dirty pubkeys
// from pending_profile_stats_recomputes using the same 3-phase
// claim/rebuild/cleanup pattern as DrainPendingAuthorAnalyticsBatch.
// See that function's doc comment for the full rationale.
//
// Phase 2 here runs BOTH ProjectProfilePublicStats AND
// ProjectProfileDiscoveryStats inside a single transaction. We use
// the profile_public_stats namespace for the phase-1 advisory-lock
// claim filter as a coarse per-pubkey mutex; the discovery_stats
// projection acquires its own (different-namespace) lock inside
// projectProfileDiscoveryStats, but because phase 1 already
// established that no other sweeper goroutine is operating on this
// pubkey, that acquisition is uncontested in the sweeper path.
//
// Returns the number of pubkeys whose recompute completed successfully
// and the first error encountered (if any). On error, the failed
// pubkey's claim is released so the next sweeper cycle retries it.
func (h *Handlers) DrainPendingProfileStatsBatch(ctx context.Context, limit int) (int, error) {
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
		ok, err := h.processNextPendingProfileStatsPubkey(ctx)
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

// processNextPendingProfileStatsPubkey runs the 3-phase
// claim/rebuild/cleanup cycle for one profile-stats pubkey. Returns
// (false, nil) when no pubkey is currently claimable.
func (h *Handlers) processNextPendingProfileStatsPubkey(ctx context.Context) (bool, error) {
	pubkey, claimToken, markedAt, ok, err := h.claimPendingProfileStatsPubkey(ctx)
	if err != nil || !ok {
		return false, err
	}

	if rebuildErr := h.rebuildClaimedProfileStatsPubkey(ctx, pubkey); rebuildErr != nil {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = h.releasePendingProfileStatsClaim(releaseCtx, pubkey, claimToken)
		releaseCancel()
		return false, fmt.Errorf("rebuild profile stats for %s: %w", pubkey, rebuildErr)
	}

	if err := h.finalizePendingProfileStatsClaim(ctx, pubkey, claimToken, markedAt); err != nil {
		return true, fmt.Errorf("finalize profile stats claim for %s: %w", pubkey, err)
	}
	return true, nil
}

// claimPendingProfileStatsPubkey runs the phase-1 claim transaction.
// Mirrors claimPendingAuthorAnalyticsPubkey; see that function for
// the rationale on each CTE stage.
func (h *Handlers) claimPendingProfileStatsPubkey(ctx context.Context) (string, string, time.Time, bool, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return "", "", time.Time{}, false, fmt.Errorf("begin profile stats claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var pubkey, claimToken string
	var markedAt time.Time
	err = tx.QueryRow(ctx, `
		WITH candidates AS (
			SELECT pubkey, marked_at
			FROM pending_profile_stats_recomputes
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
		UPDATE pending_profile_stats_recomputes p
		SET claimed_at = now(),
		    claim_token = gen_random_uuid()
		FROM locked l
		WHERE p.pubkey = l.pubkey
		RETURNING p.pubkey, p.claim_token::text, l.marked_at
	`,
		profileStatsClaimCandidateWindow,
		pubkeyLockNamespaceProfilePublicStats,
		profileStatsClaimLease,
	).Scan(&pubkey, &claimToken, &markedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", time.Time{}, false, nil
		}
		return "", "", time.Time{}, false, fmt.Errorf("claim pending profile stats pubkey: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", "", time.Time{}, false, fmt.Errorf("commit profile stats claim tx: %w", err)
	}
	return pubkey, claimToken, markedAt, true, nil
}

// rebuildClaimedProfileStatsPubkey runs the phase-2 rebuild
// transaction, performing both projections under a single tx. The
// per-pubkey advisory locks for both namespaces are acquired
// (blocking) inside the projection helpers; both acquires are
// uncontested in the sweeper path because phase 1's claim filter
// guarantees we are the only sweeper touching this pubkey.
func (h *Handlers) rebuildClaimedProfileStatsPubkey(ctx context.Context, pubkey string) error {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin profile stats rebuild tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// When incremental profile_public_stats is enabled, note/reply/follower
	// counters are maintained with O(1) deltas in the derive bundle. The
	// sweeper only needs to refresh discovery stats. Keeping the full
	// public-stats recompute available when the flag is off preserves the
	// previous behavior and the rebuild/reconciliation escape hatch.
	if !h.incrementalProfilePublicStats {
		if err := h.projectProfilePublicStatsForPubkeysTx(ctx, tx, []string{pubkey}, nil); err != nil {
			return fmt.Errorf("rebuild profile public stats: %w", err)
		}
	}

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationProfileDiscoveryStats,
		ProfileDiscoveryStatsVersion,
		"Project per-profile discovery counters and rolling scores",
		nil,
	)
	if err != nil {
		return fmt.Errorf("resolve profile discovery stats version: %w", err)
	}
	if err := h.refreshProfileDiscoveryStatsTx(ctx, tx, pubkey, writeVersion, time.Now().UTC().Unix()); err != nil {
		return fmt.Errorf("rebuild profile discovery stats: %w", err)
	}
	return tx.Commit(ctx)
}

// finalizePendingProfileStatsClaim runs the phase-3 cleanup. See
// finalizePendingAuthorAnalyticsClaim for the CAS rationale.
func (h *Handlers) finalizePendingProfileStatsClaim(ctx context.Context, pubkey, claimToken string, markedAt time.Time) error {
	tag, err := h.pool.Exec(ctx, `
		DELETE FROM pending_profile_stats_recomputes
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
	if _, err := h.pool.Exec(ctx, `
		UPDATE pending_profile_stats_recomputes
		SET claimed_at = NULL,
		    claim_token = NULL
		WHERE pubkey = $1 AND claim_token::text = $2
	`, pubkey, claimToken); err != nil {
		return fmt.Errorf("clear claim after no-op finalize: %w", err)
	}
	return nil
}

// releasePendingProfileStatsClaim is called when the rebuild fails;
// see releasePendingAuthorAnalyticsClaim for the rationale.
func (h *Handlers) releasePendingProfileStatsClaim(ctx context.Context, pubkey, claimToken string) error {
	if _, err := h.pool.Exec(ctx, `
		UPDATE pending_profile_stats_recomputes
		SET claimed_at = NULL,
		    claim_token = NULL
		WHERE pubkey = $1 AND claim_token::text = $2
	`, pubkey, claimToken); err != nil {
		return fmt.Errorf("release pending profile stats claim: %w", err)
	}
	return nil
}

// PendingProfileStatsBacklog returns the current depth of the dirty
// queue. Exposed for metrics / admin observability.
func (h *Handlers) PendingProfileStatsBacklog(ctx context.Context) (int64, error) {
	if h == nil || h.pool == nil {
		return 0, fmt.Errorf("handlers are not initialized")
	}
	var n int64
	if err := h.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pending_profile_stats_recomputes
	`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending profile stats: %w", err)
	}
	return n, nil
}
