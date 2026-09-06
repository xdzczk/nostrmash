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

// ReconciliationFailure records a pubkey whose reconciliation checks could
// not be completed (e.g. a recompute query timed out). Failures are reported
// rather than aborting the pass so one pathological account can't starve the
// correctness backstop for every other sampled pubkey.
type ReconciliationFailure struct {
	Pubkey string
	Err    error
}

// ReconciliationReport summarizes one reconciliation pass.
type ReconciliationReport struct {
	// SampledPubkeys is the number of distinct pubkeys checked.
	SampledPubkeys int
	Mismatches     []ReconciliationMismatch
	// Failures lists sampled pubkeys that were skipped because a check
	// errored; the remaining pubkeys were still reconciled.
	Failures []ReconciliationFailure
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
		// A failing pubkey (e.g. a recompute that times out on a
		// pathological account) is recorded and skipped, not fatal:
		// aborting would silently drop the checks and heals for every
		// remaining pubkey in the sample. Context cancellation is still
		// fatal — every remaining check would fail the same way.
		mismatches, err := h.reconcilePubkey(ctx, pubkey)
		if err != nil {
			if ctx.Err() != nil {
				return ReconciliationReport{}, err
			}
			report.Failures = append(report.Failures, ReconciliationFailure{Pubkey: pubkey, Err: err})
			continue
		}
		report.Mismatches = append(report.Mismatches, mismatches...)
	}
	return report, nil
}

// reconcilePubkey runs every reconciliation check for one pubkey.
func (h *Handlers) reconcilePubkey(ctx context.Context, pubkey string) ([]ReconciliationMismatch, error) {
	var mismatches []ReconciliationMismatch

	profileMismatches, err := h.reconcileProfilePublicStatsForPubkey(ctx, pubkey)
	if err != nil {
		return nil, fmt.Errorf("reconcile profile public stats for %s: %w", pubkey, err)
	}
	mismatches = append(mismatches, profileMismatches...)

	activityMismatches, err := h.reconcileAuthorActivityTotalsForPubkey(ctx, pubkey)
	if err != nil {
		return nil, fmt.Errorf("reconcile author activity totals for %s: %w", pubkey, err)
	}
	mismatches = append(mismatches, activityMismatches...)

	if h.incrementalProfileDiscoveryStats {
		discoveryMismatches, err := h.reconcileProfileDiscoveryStatsForPubkey(ctx, pubkey)
		if err != nil {
			return nil, fmt.Errorf("reconcile profile discovery stats for %s: %w", pubkey, err)
		}
		mismatches = append(mismatches, discoveryMismatches...)
	}
	return mismatches, nil
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
	// Hour-align the comparison instant. The incremental loader rolls hourly
	// buckets (a bucket is in-window when its hour start is >= the cutoff)
	// while the legacy loader filters exact created_at, so with a mid-hour
	// cutoff the two can never agree on the boundary hour and every
	// comparison carried a built-in ±(partial hour) false-mismatch band.
	// With cutoffs on an hour boundary the two window definitions select
	// exactly the same events, so any difference is real drift.
	nowUnix -= nowUnix % 3600

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
	// Only a *stale* incremental marker is a mismatch. The incremental value
	// deliberately never rolls back when retention purges the raw events
	// behind it (GREATEST-only updates), so incremental > recomputed is the
	// documented steady state after purges, not drift.
	if incActivity < fullActivity {
		appendIfDiff("recent_activity_at", incActivity, fullActivity)
	}
	return mismatches, nil
}

// maxReconciliationHealsPerRun bounds how many drifted pubkey projections a
// single reconciliation pass will rebuild. Heals for whales run the full
// per-pubkey rebuild queries, which can be expensive; anything past the cap
// is left for a later pass (the sampler will re-find it).
const maxReconciliationHealsPerRun = 8

// ReconciliationHealResult reports one attempted self-heal rebuild.
type ReconciliationHealResult struct {
	Pubkey string
	// Action identifies which rebuild ran: "profile_public_stats",
	// "author_analytics", or "discovery_recent_activity".
	Action string
	Err    error
}

// HealReconciliationMismatches repairs drifted pubkeys using the
// projections' own exact rebuild paths, so a detected mismatch is logged
// once and then fixed instead of being re-logged forever. Without healing,
// historical drift (flood incidents, purge paths that miss a delta
// reversal) plus the recency-biased sampler meant the same whale pubkeys
// produced the same mismatch log lines every pass, ~2k/day of standing
// noise that buried real regressions.
//
// Heal routing:
//   - profile_public_stats fields (and the discovery follower_count, which
//     reads from that table) → full profile_public_stats rebuild;
//   - author_activity_daily totals → full author-analytics rebuild
//     (recomputes daily rows plus every windowed author projection);
//   - discovery recent_activity_at → GREATEST-upsert of the recomputed
//     marker (stale-only by construction, so raising it is always safe);
//   - discovery window fields (hourly-bucket sourced) have no rebuild path
//     and are intentionally left as log-only signals.
//
// Adopting the recompute is consistent with system semantics: an operator
// rebuild would produce exactly the same values.
func (h *Handlers) HealReconciliationMismatches(ctx context.Context, mismatches []ReconciliationMismatch) []ReconciliationHealResult {
	if h == nil || h.pool == nil || len(mismatches) == 0 {
		return nil
	}
	type healJob struct {
		pubkey string
		action string
		// recomputed carries the target value for discovery_recent_activity.
		recomputed int64
	}
	seen := make(map[string]struct{}, len(mismatches))
	jobs := make([]healJob, 0, len(mismatches))
	add := func(job healJob) {
		key := job.action + "\x00" + job.pubkey
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		jobs = append(jobs, job)
	}
	for _, m := range mismatches {
		switch m.Projection {
		case "profile_public_stats":
			add(healJob{pubkey: m.Pubkey, action: "profile_public_stats"})
		case "author_activity_daily":
			add(healJob{pubkey: m.Pubkey, action: "author_analytics"})
		case "profile_discovery_stats":
			switch m.Field {
			case "follower_count":
				add(healJob{pubkey: m.Pubkey, action: "profile_public_stats"})
			case "recent_activity_at":
				add(healJob{pubkey: m.Pubkey, action: "discovery_recent_activity", recomputed: m.Recomputed})
			}
		}
	}
	if len(jobs) > maxReconciliationHealsPerRun {
		jobs = jobs[:maxReconciliationHealsPerRun]
	}

	results := make([]ReconciliationHealResult, 0, len(jobs))
	for _, job := range jobs {
		var err error
		switch job.action {
		case "profile_public_stats":
			err = h.rebuildProfilePublicStatsForPubkey(ctx, job.pubkey)
		case "author_analytics":
			err = h.projectAuthorAnalyticsForPubkey(ctx, job.pubkey, nil)
		case "discovery_recent_activity":
			_, err = h.pool.Exec(ctx, `
				INSERT INTO profile_discovery_recent_activity (pubkey, recent_activity_at)
				VALUES ($1, $2)
				ON CONFLICT (pubkey) DO UPDATE
				SET recent_activity_at = GREATEST(
						profile_discovery_recent_activity.recent_activity_at,
						EXCLUDED.recent_activity_at
					),
				    updated_at = now()
			`, job.pubkey, job.recomputed)
		}
		results = append(results, ReconciliationHealResult{Pubkey: job.pubkey, Action: job.action, Err: err})
	}
	return results
}

// rebuildProfilePublicStatsForPubkey runs the projection's own full rebuild
// for one pubkey in a fresh transaction.
func (h *Handlers) rebuildProfilePublicStatsForPubkey(ctx context.Context, pubkey string) error {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin profile public stats heal tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := h.projectProfilePublicStatsForPubkeysTx(ctx, tx, []string{pubkey}, nil); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile public stats heal tx: %w", err)
	}
	return nil
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
	// Stale-only, matching the discovery recency check: retention purges
	// lower the recompute but deliberately never roll the live value back.
	if incrementalActivity < recomputedActivity {
		appendIfDiff("recent_activity_at", incrementalActivity, recomputedActivity)
	}

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
				      FROM thread_edges te
				      WHERE te.child_event_id = e.id
				  )
			), 0) AS note_count,
			COALESCE((
				SELECT COUNT(*)
				FROM events e
				WHERE e.pubkey = $1
				  AND e.kind = 1
				  AND EXISTS (
				      SELECT 1
				      FROM thread_edges te
				      WHERE te.child_event_id = e.id
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
//
// Engagement counts read the target/source pubkeys denormalized onto the
// projection tables (migration 000082) rather than joining events per row.
// Beyond speed (the join plan heap-scanned every event a prolific author
// wrote, timing out on hot accounts), the denormalized columns capture what
// the projection knew at write time — exactly the information the
// incremental deltas were computed from — so comparing against them is the
// more faithful reconciliation baseline.
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
				FROM reply_count_contributions rcc
				WHERE rcc.target_pubkey = $1
				  AND rcc.source_pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM reaction_events re
				WHERE re.target_pubkey = $1
				  AND re.reactor_pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM repost_events re
				WHERE re.target_pubkey = $1
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
				FROM reply_count_contributions rcc
				WHERE rcc.source_pubkey = $1
				  AND rcc.target_pubkey IS NOT NULL
				  AND rcc.target_pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM reaction_events re
				WHERE re.reactor_pubkey = $1
				  AND re.target_pubkey IS NOT NULL
				  AND re.target_pubkey <> $1
			), 0)
			+ COALESCE((
				SELECT COUNT(*)
				FROM repost_events re
				WHERE re.reposter_pubkey = $1
				  AND re.target_pubkey IS NOT NULL
				  AND re.target_pubkey <> $1
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
