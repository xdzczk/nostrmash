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
	post24h, reply24h, engagement24h, zapVolume24h, activeDays24h, newFollowers24h, err := loadProfileWindowMetricsTx(ctx, tx, pubkey, nowUnix, 24*time.Hour)
	if err != nil {
		return err
	}
	post7d, reply7d, engagement7d, zapVolume7d, activeDays7d, newFollowers7d, err := loadProfileWindowMetricsTx(ctx, tx, pubkey, nowUnix, 7*24*time.Hour)
	if err != nil {
		return err
	}
	recentActivityAt, err := loadProfileRecentActivityAtTx(ctx, tx, pubkey)
	if err != nil {
		return err
	}
	followerCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM follower_edges
		WHERE followed_pubkey = $1
	`, pubkey)
	if err != nil {
		return fmt.Errorf("load follower count for profile discovery score: %w", err)
	}

	score24h := computeProfileTrendingScore(24*time.Hour, nowUnix, recentActivityAt, post24h, reply24h, engagement24h, zapVolume24h, activeDays24h)
	score7d := computeProfileTrendingScore(7*24*time.Hour, nowUnix, recentActivityAt, post7d, reply7d, engagement7d, zapVolume7d, activeDays7d)
	risingScore24h := computeProfileRisingScore(score24h, followerCount, newFollowers24h, engagement24h, post24h, reply24h, activeDays24h)
	risingScore7d := computeProfileRisingScore(score7d, followerCount, newFollowers7d, engagement7d, post7d, reply7d, activeDays7d)

	if score7d <= 0 && risingScore7d <= 0 && post7d == 0 && reply7d == 0 && engagement7d == 0 {
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
	`, pubkey, score24h, score7d, risingScore24h, risingScore7d, post24h, reply24h, engagement24h, zapVolume24h, activeDays24h, recentActivityAt, writeVersion); err != nil {
		return fmt.Errorf("upsert profile discovery stats: %w", err)
	}
	return nil
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

func loadProfileWindowMetricsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	nowUnix int64,
	window time.Duration,
) (int64, int64, int64, int64, int, int64, error) {
	cutoff := nowUnix - int64(window/time.Second)

	postCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM events e
		WHERE e.pubkey = $1
		  AND e.kind = 1
		  AND e.created_at >= $2
		  AND NOT EXISTS (
			SELECT 1
			FROM event_references er
			WHERE er.source_event_id = e.id
			  AND er.relation = 'reply'
		  )
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed post_count: %w", err)
	}
	replyCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM events e
		WHERE e.pubkey = $1
		  AND e.kind = 1
		  AND e.created_at >= $2
		  AND EXISTS (
			SELECT 1
			FROM event_references er
			WHERE er.source_event_id = e.id
			  AND er.relation = 'reply'
		  )
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed reply_count: %w", err)
	}
	replyReceived, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM reply_count_contributions c
		JOIN events source_event ON source_event.id = c.source_event_id
		JOIN events target_event ON target_event.id = c.target_event_id
		WHERE target_event.pubkey = $1
		  AND source_event.created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed replies received: %w", err)
	}
	repostReceived, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM repost_events r
		JOIN events target_event ON target_event.id = r.target_event_id
		WHERE target_event.pubkey = $1
		  AND r.created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed reposts received: %w", err)
	}
	reactionReceived, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM reaction_events r
		JOIN events target_event ON target_event.id = r.target_event_id
		WHERE target_event.pubkey = $1
		  AND r.created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed reactions received: %w", err)
	}
	zapCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM zap_receipts
		WHERE receiver_pubkey = $1
		  AND created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed zaps received: %w", err)
	}
	zapVolumeMSats, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(SUM(amount_sats * 1000), 0)
		FROM zap_receipts
		WHERE receiver_pubkey = $1
		  AND created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed zap volume: %w", err)
	}
	activeDaysRaw, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(DISTINCT to_timestamp(created_at)::date), 0)
		FROM events
		WHERE pubkey = $1
		  AND created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed active days: %w", err)
	}
	newFollowerCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM follower_edges
		WHERE followed_pubkey = $1
		  AND contact_list_created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed new follower count: %w", err)
	}

	engagement := replyReceived + repostReceived + reactionReceived + zapCount
	return postCount, replyCount, engagement, zapVolumeMSats, int(activeDaysRaw), newFollowerCount, nil
}
