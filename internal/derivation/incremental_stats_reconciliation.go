package derivation

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ReconciliationMismatch describes one field where the incrementally
// maintained value diverged from a fresh full-history recompute for a
// sampled pubkey.
type ReconciliationMismatch struct {
	Pubkey      string
	Projection  string
	Field       string
	Incremental int64
	Recomputed  int64
}

// ReconciliationReport summarizes one reconciliation pass.
type ReconciliationReport struct {
	// SampledPubkeys is the number of distinct pubkeys checked.
	SampledPubkeys int
	Mismatches     []ReconciliationMismatch
}

// ReconcileIncrementalAuthorStatsSample is the correctness backstop for the
// incremental author/profile stats design (see
// docs/design/incremental-author-stats.md, "Correctness backstop" section):
// it periodically full-recomputes a sample of pubkeys and compares against
// the incrementally-maintained values, without writing anything. A mismatch
// here means some fan-out path is not emitting a delta it should (or is
// emitting a wrong one) — a silent correctness bug the steady-state O(1)
// path would otherwise never surface.
//
// The sample mixes recently-active pubkeys (where a live bug would show up
// fastest and matters most) with a uniform-random slice (so a bug that only
// affects quiet/older accounts isn't perpetually invisible). Read-only:
// every check is a plain SELECT comparison, never a write, so running this
// can never itself introduce drift.
func (h *Handlers) ReconcileIncrementalAuthorStatsSample(ctx context.Context, sampleSize int) (ReconciliationReport, error) {
	if h == nil || h.pool == nil {
		return ReconciliationReport{}, fmt.Errorf("handlers are not initialized")
	}
	if sampleSize <= 0 {
		return ReconciliationReport{}, fmt.Errorf("sampleSize must be > 0")
	}

	pubkeys, err := h.sampleReconciliationPubkeys(ctx, sampleSize)
	if err != nil {
		return ReconciliationReport{}, err
	}

	report := ReconciliationReport{SampledPubkeys: len(pubkeys)}
	for _, pubkey := range pubkeys {
		profileMismatches, err := h.reconcileProfilePublicStatsForPubkey(ctx, pubkey)
		if err != nil {
			return ReconciliationReport{}, fmt.Errorf("reconcile profile public stats for %s: %w", pubkey, err)
		}
		report.Mismatches = append(report.Mismatches, profileMismatches...)

		activityMismatches, err := h.reconcileAuthorActivityTotalsForPubkey(ctx, pubkey)
		if err != nil {
			return ReconciliationReport{}, fmt.Errorf("reconcile author activity totals for %s: %w", pubkey, err)
		}
		report.Mismatches = append(report.Mismatches, activityMismatches...)

		if h.incrementalProfileDiscoveryStats {
			discoveryMismatches, err := h.reconcileProfileDiscoveryStatsForPubkey(ctx, pubkey)
			if err != nil {
				return ReconciliationReport{}, fmt.Errorf("reconcile profile discovery stats for %s: %w", pubkey, err)
			}
			report.Mismatches = append(report.Mismatches, discoveryMismatches...)
		}
	}
	return report, nil
}

// reconcileProfileDiscoveryStatsForPubkey compares the incremental daily/hourly
// rollup against the legacy full-scan metric loaders for the fields that share
// semantics. new_followers is intentionally excluded: incremental path uses
// true kind=3 edge-diff gains, while the legacy scan counts edges whose
// contact_list_created_at falls in the window (rewrites re-count).
func (h *Handlers) reconcileProfileDiscoveryStatsForPubkey(ctx context.Context, pubkey string) ([]ReconciliationMismatch, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin discovery reconciliation tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	nowUnix := 0
	// Use a stable "now" from the DB so both loaders see the same cutoffs.
	if err := tx.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM now())::bigint`).Scan(&nowUnix); err != nil {
		return nil, fmt.Errorf("load reconciliation now: %w", err)
	}

	incremental, err := loadProfileDualWindowMetricsIncrementalTx(ctx, tx, pubkey, int64(nowUnix))
	if err != nil {
		return nil, err
	}
	full, err := loadProfileDualWindowMetricsTx(ctx, tx, pubkey, int64(nowUnix))
	if err != nil {
		return nil, err
	}
	incRecent, err := loadProfileDiscoveryRecentActivityAtTx(ctx, tx, pubkey)
	if err != nil {
		return nil, err
	}
	fullRecent, err := loadProfileRecentActivityAtTx(ctx, tx, pubkey)
	if err != nil {
		return nil, err
	}

	var mismatches []ReconciliationMismatch
	appendIfDiff := func(field string, got, want int64) {
		if got != want {
			mismatches = append(mismatches, ReconciliationMismatch{
				Pubkey:      pubkey,
				Projection:  "profile_discovery_stats",
				Field:       field,
				Incremental: got,
				Recomputed:  want,
			})
		}
	}
	appendIfDiff("post_count_24h", incremental.window24h.postCount, full.window24h.postCount)
	appendIfDiff("reply_count_24h", incremental.window24h.replyCount, full.window24h.replyCount)
	appendIfDiff("engagement_24h", incremental.window24h.engagement, full.window24h.engagement)
	appendIfDiff("zap_msats_24h", incremental.window24h.zapVolumeMSats, full.window24h.zapVolumeMSats)
	appendIfDiff("active_days_24h", int64(incremental.window24h.activeDays), int64(full.window24h.activeDays))
	appendIfDiff("post_count_7d", incremental.window7d.postCount, full.window7d.postCount)
	appendIfDiff("reply_count_7d", incremental.window7d.replyCount, full.window7d.replyCount)
	appendIfDiff("engagement_7d", incremental.window7d.engagement, full.window7d.engagement)
	appendIfDiff("zap_msats_7d", incremental.window7d.zapVolumeMSats, full.window7d.zapVolumeMSats)
	appendIfDiff("active_days_7d", int64(incremental.window7d.activeDays), int64(full.window7d.activeDays))
	appendIfDiff("follower_count", incremental.followerCount, full.followerCount)

	incActivity := int64(0)
	if incRecent != nil {
		incActivity = *incRecent
	}
	fullActivity := int64(0)
	if fullRecent != nil {
		fullActivity = *fullRecent
	}
	appendIfDiff("recent_activity_at", incActivity, fullActivity)
	return mismatches, nil
}

// sampleReconciliationPubkeys draws up to sampleSize distinct pubkeys from
// profile_public_stats, split evenly between the most recently active
// pubkeys and a uniform-random slice. Half-and-half rather than pure
// recency: recently active accounts are where a live regression shows up
// fastest, but a bug scoped to quiet/backfilled accounts would otherwise
// never be sampled at all.
func (h *Handlers) sampleReconciliationPubkeys(ctx context.Context, sampleSize int) ([]string, error) {
	recentCount := (sampleSize + 1) / 2
	randomCount := sampleSize - recentCount

	rows, err := h.pool.Query(ctx, `
		(
			SELECT pubkey
			FROM profile_public_stats
			ORDER BY recent_activity_at DESC NULLS LAST, pubkey ASC
			LIMIT $1
		)
		UNION
		(
			SELECT pubkey
			FROM profile_public_stats
			ORDER BY random()
			LIMIT $2
		)
	`, recentCount, randomCount)
	if err != nil {
		return nil, fmt.Errorf("sample reconciliation pubkeys: %w", err)
	}
	defer rows.Close()

	var pubkeys []string
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, fmt.Errorf("scan reconciliation pubkey: %w", err)
		}
		pubkeys = append(pubkeys, pubkey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reconciliation pubkeys: %w", err)
	}
	return pubkeys, nil
}

// profilePublicStatsSnapshot is the comparable subset of one
// profile_public_stats row.
type profilePublicStatsSnapshot struct {
	FollowerCount    int64
	FollowingCount   int64
	NoteCount        int64
	ReplyCount       int64
	RecentActivityAt *int64
}

// reconcileProfilePublicStatsForPubkey compares the live
// profile_public_stats row for pubkey against a fresh full-history
// recompute using the exact same query
// projectProfilePublicStatsForPubkeysTx would use to rebuild it, so any
// divergence reflects a real gap in the incremental write path, not a
// difference in how "correct" is defined.
func (h *Handlers) reconcileProfilePublicStatsForPubkey(ctx context.Context, pubkey string) ([]ReconciliationMismatch, error) {
	var incremental profilePublicStatsSnapshot
	found := true
	if err := h.pool.QueryRow(ctx, `
		SELECT follower_count, following_count, note_count, reply_count, recent_activity_at
		FROM profile_public_stats
		WHERE pubkey = $1
	`, pubkey).Scan(
		&incremental.FollowerCount,
		&incremental.FollowingCount,
		&incremental.NoteCount,
		&incremental.ReplyCount,
		&incremental.RecentActivityAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			found = false
		} else {
			return nil, fmt.Errorf("load current profile public stats for %s: %w", pubkey, err)
		}
	}

	recomputed, err := h.computeTrueProfilePublicStats(ctx, pubkey)
	if err != nil {
		return nil, err
	}

	if !found {
		// A pubkey sampled from profile_public_stats always has a row (we
		// just read it from that table), so this branch is unreachable in
		// practice; kept only as a defensive guard against a
		// read-after-delete race.
		return nil, nil
	}

	var mismatches []ReconciliationMismatch
	appendIfDiff := func(field string, got, want int64) {
		if got != want {
			mismatches = append(mismatches, ReconciliationMismatch{
				Pubkey:      pubkey,
				Projection:  "profile_public_stats",
				Field:       field,
				Incremental: got,
				Recomputed:  want,
			})
		}
	}
	appendIfDiff("follower_count", incremental.FollowerCount, recomputed.FollowerCount)
	appendIfDiff("following_count", incremental.FollowingCount, recomputed.FollowingCount)
	appendIfDiff("note_count", incremental.NoteCount, recomputed.NoteCount)
	appendIfDiff("reply_count", incremental.ReplyCount, recomputed.ReplyCount)

	incrementalActivity := int64(0)
	if incremental.RecentActivityAt != nil {
		incrementalActivity = *incremental.RecentActivityAt
	}
	recomputedActivity := int64(0)
	if recomputed.RecentActivityAt != nil {
		recomputedActivity = *recomputed.RecentActivityAt
	}
	appendIfDiff("recent_activity_at", incrementalActivity, recomputedActivity)

	return mismatches, nil
}

// computeTrueProfilePublicStats runs the identical read query
// projectProfilePublicStatsForPubkeysTx uses to rebuild a row, without
// writing anything. Kept in exact sync with that function intentionally —
// any change there should be mirrored here.
func (h *Handlers) computeTrueProfilePublicStats(ctx context.Context, pubkey string) (profilePublicStatsSnapshot, error) {
	var out profilePublicStatsSnapshot
	if err := h.pool.QueryRow(ctx, `
		SELECT
			COALESCE((
				SELECT COUNT(*)
				FROM follower_edges
				WHERE followed_pubkey = $1
			), 0) AS follower_count,
			COALESCE((
				SELECT COUNT(*)
				FROM follower_edges
				WHERE follower_pubkey = $1
			), 0) AS following_count,
			COALESCE((
				SELECT COUNT(*)
				FROM events e
				WHERE e.pubkey = $1
				  AND e.kind = 1
				  AND NOT EXISTS (
				      SELECT 1
				      FROM event_references er
				      WHERE er.source_event_id = e.id
				        AND er.relation = 'reply'
				  )
			), 0) AS note_count,
			COALESCE((
				SELECT COUNT(*)
				FROM events e
				WHERE e.pubkey = $1
				  AND e.kind = 1
				  AND EXISTS (
				      SELECT 1
				      FROM event_references er
				      WHERE er.source_event_id = e.id
				        AND er.relation = 'reply'
				  )
			), 0) AS reply_count,
			(
				SELECT MAX(created_at)
				FROM events
				WHERE pubkey = $1
			) AS recent_activity_at
	`, pubkey).Scan(
		&out.FollowerCount,
		&out.FollowingCount,
		&out.NoteCount,
		&out.ReplyCount,
		&out.RecentActivityAt,
	); err != nil {
		return profilePublicStatsSnapshot{}, fmt.Errorf("compute true profile public stats for %s: %w", pubkey, err)
	}
	return out, nil
}

// authorActivityTotalsSnapshot is the all-history sum of the counters
// author_activity_daily rows split up by day.
type authorActivityTotalsSnapshot struct {
	PostCount          int64
	EngagementReceived int64
	EngagementGiven    int64
}

// reconcileAuthorActivityTotalsForPubkey compares the all-history sum of
// author_activity_daily against an independent full recompute over the same
// base tables rebuildAuthorActivityDailyTx reads. This is a coarser check
// than the per-day rebuild (it cannot catch an event landing in the wrong
// day bucket), but it directly catches the failure mode that matters most
// for an O(1) delta system: a fan-out path silently failing to apply (or
// double-applying) a delta.
func (h *Handlers) reconcileAuthorActivityTotalsForPubkey(ctx context.Context, pubkey string) ([]ReconciliationMismatch, error) {
	var incremental authorActivityTotalsSnapshot
	if err := h.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(post_count), 0),
			COALESCE(SUM(engagement_received), 0),
			COALESCE(SUM(engagement_given), 0)
		FROM author_activity_daily
		WHERE pubkey = $1
	`, pubkey).Scan(
		&incremental.PostCount,
		&incremental.EngagementReceived,
		&incremental.EngagementGiven,
	); err != nil {
		return nil, fmt.Errorf("load current author activity totals for %s: %w", pubkey, err)
	}

	recomputed, err := h.computeTrueAuthorActivityTotals(ctx, pubkey)
	if err != nil {
		return nil, err
	}

	var mismatches []ReconciliationMismatch
	appendIfDiff := func(field string, got, want int64) {
		if got != want {
			mismatches = append(mismatches, ReconciliationMismatch{
				Pubkey:      pubkey,
				Projection:  "author_activity_daily",
				Field:       field,
				Incremental: got,
				Recomputed:  want,
			})
		}
	}
	appendIfDiff("post_count_total", incremental.PostCount, recomputed.PostCount)
	appendIfDiff("engagement_received_total", incremental.EngagementReceived, recomputed.EngagementReceived)
	appendIfDiff("engagement_given_total", incremental.EngagementGiven, recomputed.EngagementGiven)
	return mismatches, nil
}

// computeTrueAuthorActivityTotals independently recomputes the all-history
// totals rebuildAuthorActivityDailyTx would produce (summed across every
// day), without grouping by date and without writing anything.
func (h *Handlers) computeTrueAuthorActivityTotals(ctx context.Context, pubkey string) (authorActivityTotalsSnapshot, error) {
	var out authorActivityTotalsSnapshot
	if err := h.pool.QueryRow(ctx, `
		SELECT
			COALESCE((
				SELECT COUNT(*)
				FROM events e
				WHERE e.pubkey = $1
				  AND e.kind = 1
			), 0) AS post_count,
			COALESCE((
				SELECT COUNT(*)
				FROM event_references er
				INNER JOIN events e ON e.id = er.source_event_id
				INNER JOIN events target ON target.id = er.referenced_event_id
				WHERE er.relation = 'reply'
				  AND target.pubkey = $1
				  AND e.pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM reaction_events re
				INNER JOIN events target ON target.id = re.target_event_id
				WHERE target.pubkey = $1
				  AND re.reactor_pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM repost_events re
				INNER JOIN events target ON target.id = re.target_event_id
				WHERE target.pubkey = $1
				  AND re.reposter_pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM zap_receipts zr
				WHERE zr.receiver_pubkey = $1
				  AND zr.sender_pubkey IS NOT NULL
				  AND zr.sender_pubkey <> $1
			), 0) AS engagement_received,
			COALESCE((
				SELECT COUNT(*)
				FROM event_references er
				INNER JOIN events e ON e.id = er.source_event_id
				INNER JOIN events target ON target.id = er.referenced_event_id
				WHERE er.relation = 'reply'
				  AND e.pubkey = $1
				  AND target.pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM reaction_events re
				INNER JOIN events target ON target.id = re.target_event_id
				WHERE re.reactor_pubkey = $1
				  AND target.pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM repost_events re
				INNER JOIN events target ON target.id = re.target_event_id
				WHERE re.reposter_pubkey = $1
				  AND target.pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM zap_receipts zr
				WHERE zr.sender_pubkey = $1
				  AND zr.receiver_pubkey <> $1
			), 0) AS engagement_given
	`, pubkey).Scan(
		&out.PostCount,
		&out.EngagementReceived,
		&out.EngagementGiven,
	); err != nil {
		return authorActivityTotalsSnapshot{}, fmt.Errorf("compute true author activity totals for %s: %w", pubkey, err)
	}
	return out, nil
}
