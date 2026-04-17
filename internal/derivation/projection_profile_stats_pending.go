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

// DrainPendingProfileStatsBatch claims up to limit dirty pubkeys from
// pending_profile_stats_recomputes using FOR UPDATE SKIP LOCKED, runs
// BOTH ProjectProfilePublicStats and ProjectProfileDiscoveryStats for
// each, and removes them from the pending table on success.
//
// Mirrors DrainPendingAuthorAnalyticsBatch: parallel-safe via SKIP
// LOCKED, per-pubkey isolation so a single failure doesn't stall the
// batch, automatic re-mark of failed pubkeys for retry on the next
// cycle.
func (h *Handlers) DrainPendingProfileStatsBatch(ctx context.Context, limit int) (int, error) {
	if h == nil || h.pool == nil {
		return 0, fmt.Errorf("handlers are not initialized")
	}
	if limit <= 0 {
		return 0, nil
	}

	pubkeys, err := h.claimPendingProfileStatsPubkeys(ctx, limit)
	if err != nil {
		return 0, err
	}
	if len(pubkeys) == 0 {
		return 0, nil
	}

	processed := 0
	var firstErr error
	for _, pubkey := range pubkeys {
		if err := h.recomputeProfileStatsForPubkey(ctx, pubkey); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("recompute profile stats for %s: %w", pubkey, err)
			}
			if _, reinsertErr := h.pool.Exec(ctx, `
				INSERT INTO pending_profile_stats_recomputes (pubkey)
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

// recomputeProfileStatsForPubkey runs both projections for a single
// pubkey in a single transaction. We acquire the per-pubkey advisory
// locks (one per namespace) so any in-flight rebuild from another
// worker — or any future inline projection still using these locks —
// remains serialized, but contention here is only between sweeper
// goroutines for the same pubkey, not against the bundle critical
// path.
func (h *Handlers) recomputeProfileStatsForPubkey(ctx context.Context, pubkey string) error {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin profile stats sweeper tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := h.projectProfilePublicStatsForPubkeysTx(ctx, tx, []string{pubkey}, nil); err != nil {
		return err
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
		return err
	}
	if err := h.refreshProfileDiscoveryStatsTx(ctx, tx, pubkey, writeVersion, time.Now().UTC().Unix()); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile stats sweeper tx: %w", err)
	}
	return nil
}

// claimPendingProfileStatsPubkeys atomically claims up to limit dirty
// pubkeys using SELECT ... FOR UPDATE SKIP LOCKED followed by DELETE.
// Same crash-window trade-off as the author-analytics sweeper: a crash
// between the DELETE-commit and the recompute-commit drops the dirty
// signal, but the next event from the pubkey re-marks it so data
// converges.
func (h *Handlers) claimPendingProfileStatsPubkeys(ctx context.Context, limit int) ([]string, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin pending profile stats claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		WITH claimed AS (
			SELECT pubkey
			FROM pending_profile_stats_recomputes
			ORDER BY marked_at ASC, pubkey ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		DELETE FROM pending_profile_stats_recomputes p
		USING claimed c
		WHERE p.pubkey = c.pubkey
		RETURNING p.pubkey
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim pending profile stats pubkeys: %w", err)
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
		return nil, fmt.Errorf("commit pending profile stats claim: %w", err)
	}
	return pubkeys, nil
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
