package derivation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) ProjectProfileDiscoveryStats(ctx context.Context, eventID string) error {
	return h.projectProfileDiscoveryStatsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectProfileDiscoveryStatsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
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
		return fmt.Errorf("load event for profile discovery projection: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	references := deriveEventReferences(eventID, tags)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationProfileDiscoveryStats,
		ProfileDiscoveryStatsVersion,
		"Project per-profile discovery counters and rolling scores",
		versionOverride,
	)
	if err != nil {
		return err
	}

	affectedPubkeys, err := h.affectedProfileDiscoveryPubkeysTx(ctx, tx, eventID, kind, references, tags)
	if err != nil {
		return err
	}
	nowUnix := time.Now().UTC().Unix()
	for _, pubkey := range affectedPubkeys {
		if err := h.refreshProfileDiscoveryStatsTx(ctx, tx, pubkey, writeVersion, nowUnix); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile discovery projection tx: %w", err)
	}
	return nil
}

func (h *Handlers) refreshProfileDiscoveryStatsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	writeVersion int,
	nowUnix int64,
) error {
	if err := lockPubkeyForWriteTx(ctx, tx, pubkey, pubkeyLockNamespaceProfileDiscoveryStats); err != nil {
		return err
	}

	var (
		metrics          profileDualWindowMetrics
		recentActivityAt *int64
		err              error
	)
	if h.incrementalProfileDiscoveryStats {
		metrics, err = loadProfileDualWindowMetricsIncrementalTx(ctx, tx, pubkey, nowUnix)
		if err != nil {
			return err
		}
		recentActivityAt, err = loadProfileDiscoveryRecentActivityAtTx(ctx, tx, pubkey)
		if err != nil {
			return err
		}
	} else {
		metrics, err = loadProfileDualWindowMetricsTx(ctx, tx, pubkey, nowUnix)
		if err != nil {
			return err
		}
		recentActivityAt, err = loadProfileRecentActivityAtTx(ctx, tx, pubkey)
		if err != nil {
			return err
		}
	}

	w24 := metrics.window24h
	w7 := metrics.window7d
	score24h := computeProfileTrendingScore(24*time.Hour, nowUnix, recentActivityAt, w24.postCount, w24.replyCount, w24.engagement, w24.zapVolumeMSats, w24.activeDays)
	score7d := computeProfileTrendingScore(7*24*time.Hour, nowUnix, recentActivityAt, w7.postCount, w7.replyCount, w7.engagement, w7.zapVolumeMSats, w7.activeDays)
	risingScore24h := computeProfileRisingScore(score24h, metrics.followerCount, w24.newFollowers, w24.engagement, w24.postCount, w24.replyCount, w24.activeDays)
	risingScore7d := computeProfileRisingScore(score7d, metrics.followerCount, w7.newFollowers, w7.engagement, w7.postCount, w7.replyCount, w7.activeDays)

	if score7d <= 0 && risingScore7d <= 0 && w7.postCount == 0 && w7.replyCount == 0 && w7.engagement == 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM profile_discovery_stats WHERE pubkey = $1`, pubkey); err != nil {
			return fmt.Errorf("delete stale profile discovery row: %w", err)
		}
		return nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO profile_discovery_stats (
			pubkey,
			score_24h,
			score_7d,
			rising_score_24h,
			rising_score_7d,
			recent_post_count,
			recent_reply_count,
			recent_engagement_received,
			recent_zap_volume_msats,
			recent_active_days,
			recent_activity_at,
			last_scored_at,
			derivation_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), $12)
		ON CONFLICT (pubkey) DO UPDATE
		SET score_24h = EXCLUDED.score_24h,
		    score_7d = EXCLUDED.score_7d,
		    rising_score_24h = EXCLUDED.rising_score_24h,
		    rising_score_7d = EXCLUDED.rising_score_7d,
		    recent_post_count = EXCLUDED.recent_post_count,
		    recent_reply_count = EXCLUDED.recent_reply_count,
		    recent_engagement_received = EXCLUDED.recent_engagement_received,
		    recent_zap_volume_msats = EXCLUDED.recent_zap_volume_msats,
		    recent_active_days = EXCLUDED.recent_active_days,
		    recent_activity_at = EXCLUDED.recent_activity_at,
		    last_scored_at = now(),
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`, pubkey, score24h, score7d, risingScore24h, risingScore7d, w24.postCount, w24.replyCount, w24.engagement, w24.zapVolumeMSats, w24.activeDays, recentActivityAt, writeVersion); err != nil {
		return fmt.Errorf("upsert profile discovery stats: %w", err)
	}
	return nil
}

// loadProfileDiscoveryRecentActivityAtTx reads the O(1)-maintained discovery
// recency marker, falling back to profile_public_stats.recent_activity_at
// (author-only) when no discovery-specific row exists yet.
func loadProfileDiscoveryRecentActivityAtTx(ctx context.Context, tx pgx.Tx, pubkey string) (*int64, error) {
	var recentActivityAt *int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(d.recent_activity_at, p.recent_activity_at)
		FROM (SELECT $1::text AS pubkey) k
		LEFT JOIN profile_discovery_recent_activity d ON d.pubkey = k.pubkey
		LEFT JOIN profile_public_stats p ON p.pubkey = k.pubkey
	`, pubkey).Scan(&recentActivityAt); err != nil {
		return nil, fmt.Errorf("load incremental profile discovery recent activity: %w", err)
	}
	return recentActivityAt, nil
}

// loadProfileDualWindowMetricsIncrementalTx rolls 24h/7d discovery windows
// from author_hourly_activity / author_activity_daily / follower_gains_daily
// and reads follower_count from profile_public_stats. Bounded to a few dozen
// indexed rows per pubkey instead of scanning raw engagement tables.
func loadProfileDualWindowMetricsIncrementalTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	nowUnix int64,
) (profileDualWindowMetrics, error) {
	cutoff24h := nowUnix - int64((24*time.Hour)/time.Second)
	cutoff7d := nowUnix - int64((7*24*time.Hour)/time.Second)
	cutoff7dDate := time.Unix(cutoff7d, 0).UTC().Truncate(24 * time.Hour)
	cutoff24hDate := time.Unix(cutoff24h, 0).UTC().Truncate(24 * time.Hour)

	var out profileDualWindowMetrics
	var activeDays24h, activeDays7d int64

	// 24h/7d authored metrics + engagement/zaps from hourly buckets whose hour
	// start falls inside each rolling window. The LEFT JOIN from a dummy row
	// keeps the query returning exactly one row when the pubkey has no hourly
	// activity yet (plain FROM author_hourly_activity would yield ErrNoRows).
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(h.note_count) FILTER (WHERE h.bucket_start >= $2), 0),
			COALESCE(SUM(h.reply_count) FILTER (WHERE h.bucket_start >= $2), 0),
			COALESCE(SUM(h.engagement_received) FILTER (WHERE h.bucket_start >= $2), 0),
			COALESCE(SUM(h.zap_msats_received) FILTER (WHERE h.bucket_start >= $2), 0),
			COALESCE(COUNT(DISTINCT h.activity_date) FILTER (
				WHERE h.bucket_start >= $2 AND h.authored_event_count > 0
			), 0),
			COALESCE(SUM(h.note_count) FILTER (WHERE h.bucket_start >= $3), 0),
			COALESCE(SUM(h.reply_count) FILTER (WHERE h.bucket_start >= $3), 0),
			COALESCE(SUM(h.engagement_received) FILTER (WHERE h.bucket_start >= $3), 0),
			COALESCE(SUM(h.zap_msats_received) FILTER (WHERE h.bucket_start >= $3), 0),
			COALESCE(COUNT(DISTINCT h.activity_date) FILTER (
				WHERE h.bucket_start >= $3 AND h.authored_event_count > 0
			), 0)
		FROM (SELECT 1) AS _
		LEFT JOIN (
			SELECT
				activity_date,
				note_count,
				reply_count,
				engagement_received,
				zap_msats_received,
				authored_event_count,
				(
					EXTRACT(EPOCH FROM (activity_date::timestamp AT TIME ZONE 'UTC'))::bigint
					+ hour_of_day::bigint * 3600
				) AS bucket_start
			FROM author_hourly_activity
			WHERE pubkey = $1
			  AND activity_date >= $4::date
		) h ON TRUE
	`, pubkey, cutoff24h, cutoff7d, cutoff7dDate).Scan(
		&out.window24h.postCount,
		&out.window24h.replyCount,
		&out.window24h.engagement,
		&out.window24h.zapVolumeMSats,
		&activeDays24h,
		&out.window7d.postCount,
		&out.window7d.replyCount,
		&out.window7d.engagement,
		&out.window7d.zapVolumeMSats,
		&activeDays7d,
	); err != nil {
		return profileDualWindowMetrics{}, fmt.Errorf("load incremental dual-window hourly metrics: %w", err)
	}
	out.window24h.activeDays = int(activeDays24h)
	out.window7d.activeDays = int(activeDays7d)

	// Follower gains use calendar-day buckets (kind=3 created_at date). That
	// matches how gains are written and is the intended rising-score input;
	// it is deliberately NOT reconciled against the legacy
	// contact_list_created_at edge scan (rewrites re-count there).
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE((
				SELECT SUM(gained) FROM follower_gains_daily
				WHERE pubkey = $1 AND activity_date >= $2::date
			), 0),
			COALESCE((
				SELECT SUM(gained) FROM follower_gains_daily
				WHERE pubkey = $1 AND activity_date >= $3::date
			), 0),
			COALESCE((
				SELECT follower_count FROM profile_public_stats WHERE pubkey = $1
			), 0)
	`, pubkey, cutoff24hDate, cutoff7dDate).Scan(
		&out.window24h.newFollowers,
		&out.window7d.newFollowers,
		&out.followerCount,
	); err != nil {
		return profileDualWindowMetrics{}, fmt.Errorf("load incremental follower discovery metrics: %w", err)
	}
	return out, nil
}

func loadProfileRecentActivityAtTx(ctx context.Context, tx pgx.Tx, pubkey string) (*int64, error) {
	var recentActivityAt *int64
	if err := tx.QueryRow(ctx, `
		SELECT MAX(created_at)
		FROM (
			SELECT e.created_at
			FROM events e
			WHERE e.pubkey = $1
			UNION ALL
			SELECT source_event.created_at
			FROM reply_count_contributions c
			JOIN events source_event ON source_event.id = c.source_event_id
			JOIN events target_event ON target_event.id = c.target_event_id
			WHERE target_event.pubkey = $1
			UNION ALL
			SELECT r.created_at
			FROM reaction_events r
			JOIN events target_event ON target_event.id = r.target_event_id
			WHERE target_event.pubkey = $1
			UNION ALL
			SELECT r.created_at
			FROM repost_events r
			JOIN events target_event ON target_event.id = r.target_event_id
			WHERE target_event.pubkey = $1
			UNION ALL
			SELECT zr.created_at
			FROM zap_receipts zr
			WHERE zr.receiver_pubkey = $1
		) activity
	`, pubkey).Scan(&recentActivityAt); err != nil {
		return nil, fmt.Errorf("load profile recent activity timestamp: %w", err)
	}
	return recentActivityAt, nil
}

type profileWindowMetrics struct {
	postCount      int64
	replyCount     int64
	engagement     int64
	zapVolumeMSats int64
	activeDays     int
	newFollowers   int64
}

type profileDualWindowMetrics struct {
	window24h     profileWindowMetrics
	window7d      profileWindowMetrics
	followerCount int64
}

// loadProfileDualWindowMetricsTx collapses the previous ~20 sequential
// COUNT round-trips (9 metrics × 2 windows + follower total) into two
// FILTER-aggregate queries over the 7d floor (superset of 24h). Semantics
// match the prior per-window loaders: no self-engagement exclusion, replies
// via reply_count_contributions, zap key = receiver_pubkey.
func loadProfileDualWindowMetricsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	nowUnix int64,
) (profileDualWindowMetrics, error) {
	cutoff24h := nowUnix - int64((24*time.Hour)/time.Second)
	cutoff7d := nowUnix - int64((7*24*time.Hour)/time.Second)

	var out profileDualWindowMetrics
	var activeDays24h, activeDays7d int64
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(COUNT(*) FILTER (
				WHERE a.kind = 1 AND a.created_at >= $2 AND NOT a.is_reply
			), 0),
			COALESCE(COUNT(*) FILTER (
				WHERE a.kind = 1 AND a.created_at >= $2 AND a.is_reply
			), 0),
			COALESCE(COUNT(*) FILTER (
				WHERE a.kind = 1 AND a.created_at >= $3 AND NOT a.is_reply
			), 0),
			COALESCE(COUNT(*) FILTER (
				WHERE a.kind = 1 AND a.created_at >= $3 AND a.is_reply
			), 0),
			COALESCE(COUNT(DISTINCT to_timestamp(a.created_at)::date) FILTER (
				WHERE a.created_at >= $2 AND a.created_at <= $4
			), 0),
			COALESCE(COUNT(DISTINCT to_timestamp(a.created_at)::date) FILTER (
				WHERE a.created_at >= $3 AND a.created_at <= $4
			), 0)
		FROM (
			SELECT
				e.created_at,
				e.kind,
				EXISTS (
					SELECT 1
					FROM event_references er
					WHERE er.source_event_id = e.id
					  AND er.relation = 'reply'
				) AS is_reply
			FROM events e
			WHERE e.pubkey = $1
			  AND e.created_at >= $3
		) a
	`, pubkey, cutoff24h, cutoff7d, maxSaneUnixCreatedAt).Scan(
		&out.window24h.postCount,
		&out.window24h.replyCount,
		&out.window7d.postCount,
		&out.window7d.replyCount,
		&activeDays24h,
		&activeDays7d,
	); err != nil {
		return profileDualWindowMetrics{}, fmt.Errorf("load profile dual-window authored metrics: %w", err)
	}
	out.window24h.activeDays = int(activeDays24h)
	out.window7d.activeDays = int(activeDays7d)

	var engagement24h, engagement7d int64
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(COUNT(*) FILTER (
				WHERE s.src IN ('reply', 'repost', 'reaction', 'zap') AND s.ts >= $2
			), 0),
			COALESCE(COUNT(*) FILTER (
				WHERE s.src IN ('reply', 'repost', 'reaction', 'zap') AND s.ts >= $3
			), 0),
			COALESCE(SUM(s.zap_msats) FILTER (WHERE s.src = 'zap' AND s.ts >= $2), 0),
			COALESCE(SUM(s.zap_msats) FILTER (WHERE s.src = 'zap' AND s.ts >= $3), 0),
			COALESCE(COUNT(*) FILTER (WHERE s.src = 'new_follow' AND s.ts >= $2), 0),
			COALESCE(COUNT(*) FILTER (WHERE s.src = 'new_follow' AND s.ts >= $3), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM follower_edges fe
				WHERE fe.followed_pubkey = $1
			), 0)
		FROM (
			SELECT 'reply'::text AS src, source_event.created_at AS ts, 0::bigint AS zap_msats
			FROM reply_count_contributions c
			JOIN events source_event ON source_event.id = c.source_event_id
			JOIN events target_event ON target_event.id = c.target_event_id
			WHERE target_event.pubkey = $1
			  AND source_event.created_at >= $3
			UNION ALL
			SELECT 'repost', r.created_at, 0
			FROM repost_events r
			JOIN events target_event ON target_event.id = r.target_event_id
			WHERE target_event.pubkey = $1
			  AND r.created_at >= $3
			UNION ALL
			SELECT 'reaction', r.created_at, 0
			FROM reaction_events r
			JOIN events target_event ON target_event.id = r.target_event_id
			WHERE target_event.pubkey = $1
			  AND r.created_at >= $3
			UNION ALL
			SELECT 'zap', zr.created_at, zr.amount_sats * 1000
			FROM zap_receipts zr
			WHERE zr.receiver_pubkey = $1
			  AND zr.created_at >= $3
			UNION ALL
			SELECT 'new_follow', fe.contact_list_created_at, 0
			FROM follower_edges fe
			WHERE fe.followed_pubkey = $1
			  AND fe.contact_list_created_at >= $3
		) s
	`, pubkey, cutoff24h, cutoff7d).Scan(
		&engagement24h,
		&engagement7d,
		&out.window24h.zapVolumeMSats,
		&out.window7d.zapVolumeMSats,
		&out.window24h.newFollowers,
		&out.window7d.newFollowers,
		&out.followerCount,
	); err != nil {
		return profileDualWindowMetrics{}, fmt.Errorf("load profile dual-window received metrics: %w", err)
	}
	out.window24h.engagement = engagement24h
	out.window7d.engagement = engagement7d
	return out, nil
}
