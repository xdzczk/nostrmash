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
// Both projections do per-pubkey aggregation queries that are expensive
// on hot pubkeys: ProjectProfilePublicStats does four sub-COUNTs over
// follower_edges + events (with a NOT EXISTS / EXISTS lookup against
// event_references); ProjectProfileDiscoveryStats does ten windowed
// COUNTs over events + reply_count_contributions + repost_events +
// reaction_events + zap_receipts. Each runs under a per-pubkey advisory
// lock keyed by namespace, so concurrent bundle workers processing
// events from the same hot pubkey serialized on the lock and ran the
// aggregates one after another. Production observed advisory-lock
// waits of 8-13 seconds per worker with the underlying COUNT queries
// running 19-30 seconds.
//
// The new design: the bundle does a cheap upsert into
// pending_profile_stats_recomputes for each affected pubkey.
// DrainPendingProfileStatsBatch runs both projections per dirty pubkey
// out-of-band, naturally coalescing bursts (N events affecting the
// same pubkey collapse into a single recompute per sweeper cycle).
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
		ON CONFLICT (pubkey) DO NOTHING
	`, pubkeys); err != nil {
		return fmt.Errorf("mark profile stats dirty: %w", err)
	}
	return nil
}

// profileStatsClaimCandidateWindow caps the candidate-row scan inside
// the atomic claim+lock query. See author_analytics' equivalent
// constant for rationale.
const profileStatsClaimCandidateWindow = 32

// DrainPendingProfileStatsBatch processes up to limit dirty pubkeys
// from pending_profile_stats_recomputes. Each pubkey is claimed and
// rebuilt in a single atomic transaction that holds the per-pubkey
// advisory lock for the entire rebuild duration.
//
// See DrainPendingAuthorAnalyticsBatch for the rationale behind the
// atomic claim+lock+rebuild pattern (avoids the lock-chain pathology
// where multiple sweeper goroutines pick the same hot pubkey and
// serialize on its advisory lock, monopolizing the pgx connection
// pool).
//
// Returns the number of pubkeys whose recompute completed successfully
// and the first error encountered (if any). On error, the failed
// pubkey's transaction rolled back, leaving its row in
// pending_profile_stats_recomputes for retry on the next cycle.
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

// processNextPendingProfileStatsPubkey atomically claims one dirty
// pubkey (using row-level SKIP LOCKED + try-advisory-lock as a filter)
// and runs both projections within the same transaction.
//
// The claim filter uses the profile_public_stats namespace as a coarse
// per-pubkey mutex: a goroutine can only claim a pubkey it can
// immediately lock on that namespace. Once claimed, the goroutine
// proceeds to acquire the profile_discovery_stats namespace lock too
// (via the inner refreshProfileDiscoveryStatsTx call) — that
// acquisition is guaranteed not to block because the public_stats
// claim filter ensures exactly one sweeper goroutine is operating on
// this pubkey at a time, so no other goroutine can be holding the
// discovery_stats lock for it either.
//
// On rebuild failure, the deferred rollback restores the
// pending_profile_stats_recomputes row, so the pubkey is automatically
// retried on the next sweeper cycle without any explicit re-mark.
//
// Returns (false, nil) when there are no claimable pubkeys (queue
// empty, or every top-of-queue pubkey is already locked by another
// goroutine).
func (h *Handlers) processNextPendingProfileStatsPubkey(ctx context.Context) (bool, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin pending profile stats processing tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var pubkey string
	err = tx.QueryRow(ctx, `
		WITH candidates AS (
			SELECT pubkey
			FROM pending_profile_stats_recomputes
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
		DELETE FROM pending_profile_stats_recomputes p
		USING locked l
		WHERE p.pubkey = l.pubkey
		RETURNING p.pubkey
	`, profileStatsClaimCandidateWindow, pubkeyLockNamespaceProfilePublicStats).Scan(&pubkey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("claim+lock pending profile stats pubkey: %w", err)
	}

	// projectProfilePublicStatsForPubkeysTx re-acquires the
	// profile_public_stats lock for this pubkey. Postgres advisory
	// locks are reentrant within a transaction, so this is a no-op
	// cost beyond a single SELECT round-trip and remains in place to
	// preserve correctness for any other call path.
	if err := h.projectProfilePublicStatsForPubkeysTx(ctx, tx, []string{pubkey}, nil); err != nil {
		return false, fmt.Errorf("rebuild profile public stats for %s: %w", pubkey, err)
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
		return false, fmt.Errorf("resolve profile discovery stats version for %s: %w", pubkey, err)
	}
	if err := h.refreshProfileDiscoveryStatsTx(ctx, tx, pubkey, writeVersion, time.Now().UTC().Unix()); err != nil {
		return false, fmt.Errorf("rebuild profile discovery stats for %s: %w", pubkey, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit profile stats rebuild for %s: %w", pubkey, err)
	}
	return true, nil
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
