package derivation

import (
	"context"
	"errors"
	"fmt"
	"math"
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

func (h *Handlers) affectedProfileDiscoveryPubkeysTx(
	ctx context.Context,
	tx pgx.Tx,
	sourceEventID string,
	kind int,
	references []derivedReference,
	tags [][]string,
) ([]string, error) {
	pubkeys := make([]string, 0, 8)

	var sourcePubkey string
	if err := tx.QueryRow(ctx, `
		SELECT pubkey
		FROM events
		WHERE id = $1
	`, sourceEventID).Scan(&sourcePubkey); err != nil {
		return nil, fmt.Errorf("load source event pubkey: %w", err)
	}
	pubkeys = append(pubkeys, sourcePubkey)

	switch kind {
	case 1:
		for _, ref := range references {
			if ref.Relation != "reply" {
				continue
			}
			targetPubkey, err := h.eventPubkeyTx(ctx, tx, ref.Referenced)
			if err != nil {
				return nil, err
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		rows, err := tx.Query(ctx, `
			SELECT e.pubkey
			FROM reply_count_contributions c
			JOIN events e ON e.id = c.target_event_id
			WHERE c.source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing profile discovery reply targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetPubkey string
			if err := rows.Scan(&targetPubkey); err != nil {
				return nil, fmt.Errorf("scan existing profile discovery reply target: %w", err)
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing profile discovery reply targets: %w", err)
		}
	case 6:
		for _, ref := range references {
			targetPubkey, err := h.eventPubkeyTx(ctx, tx, ref.Referenced)
			if err != nil {
				return nil, err
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		rows, err := tx.Query(ctx, `
			SELECT e.pubkey
			FROM repost_events r
			JOIN events e ON e.id = r.target_event_id
			WHERE r.source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing profile discovery repost targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetPubkey string
			if err := rows.Scan(&targetPubkey); err != nil {
				return nil, fmt.Errorf("scan existing profile discovery repost target: %w", err)
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing profile discovery repost targets: %w", err)
		}
	case 7:
		for _, ref := range references {
			targetPubkey, err := h.eventPubkeyTx(ctx, tx, ref.Referenced)
			if err != nil {
				return nil, err
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		rows, err := tx.Query(ctx, `
			SELECT e.pubkey
			FROM reaction_events r
			JOIN events e ON e.id = r.target_event_id
			WHERE r.source_event_id = $1
		`, sourceEventID)
		if err != nil {
			return nil, fmt.Errorf("load existing profile discovery reaction targets: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var targetPubkey string
			if err := rows.Scan(&targetPubkey); err != nil {
				return nil, fmt.Errorf("scan existing profile discovery reaction target: %w", err)
			}
			pubkeys = append(pubkeys, targetPubkey)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read existing profile discovery reaction targets: %w", err)
		}
	case 9735:
		pubkeys = append(pubkeys, firstTagValue(tags, "p"))
		var priorReceiverPubkey *string
		if err := tx.QueryRow(ctx, `
			SELECT receiver_pubkey
			FROM zap_receipts
			WHERE zap_receipt_id = $1
		`, sourceEventID).Scan(&priorReceiverPubkey); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("load prior profile discovery zap receiver: %w", err)
		}
		if priorReceiverPubkey != nil {
			pubkeys = append(pubkeys, *priorReceiverPubkey)
		}
	}
	return normalizeUniqueIDs(pubkeys), nil
}

func (h *Handlers) eventPubkeyTx(ctx context.Context, tx pgx.Tx, eventID string) (string, error) {
	var pubkey string
	if err := tx.QueryRow(ctx, `
		SELECT pubkey
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("load target event pubkey: %w", err)
	}
	return pubkey, nil
}

func (h *Handlers) refreshProfileDiscoveryStatsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	writeVersion int,
	nowUnix int64,
) error {
	post24h, reply24h, engagement24h, zapVolume24h, activeDays24h, err := loadProfileWindowMetricsTx(ctx, tx, pubkey, nowUnix, 24*time.Hour)
	if err != nil {
		return err
	}
	post7d, reply7d, engagement7d, zapVolume7d, activeDays7d, err := loadProfileWindowMetricsTx(ctx, tx, pubkey, nowUnix, 7*24*time.Hour)
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
	risingScore24h := computeProfileRisingScore(score24h, followerCount, engagement24h, post24h, reply24h, activeDays24h)
	risingScore7d := computeProfileRisingScore(score7d, followerCount, engagement7d, post7d, reply7d, activeDays7d)

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
		FROM events
		WHERE pubkey = $1
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
) (int64, int64, int64, int64, int, error) {
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
		return 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed post_count: %w", err)
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
		return 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed reply_count: %w", err)
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
		return 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed replies received: %w", err)
	}
	repostReceived, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM repost_events r
		JOIN events target_event ON target_event.id = r.target_event_id
		WHERE target_event.pubkey = $1
		  AND r.created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed reposts received: %w", err)
	}
	reactionReceived, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM reaction_events r
		JOIN events target_event ON target_event.id = r.target_event_id
		WHERE target_event.pubkey = $1
		  AND r.created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed reactions received: %w", err)
	}
	zapCount, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(*), 0)
		FROM zap_receipts
		WHERE receiver_pubkey = $1
		  AND created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed zaps received: %w", err)
	}
	zapVolumeMSats, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(SUM(amount_sats * 1000), 0)
		FROM zap_receipts
		WHERE receiver_pubkey = $1
		  AND created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed zap volume: %w", err)
	}
	activeDaysRaw, err := queryInt64Tx(ctx, tx, `
		SELECT COALESCE(COUNT(DISTINCT to_timestamp(created_at)::date), 0)
		FROM events
		WHERE pubkey = $1
		  AND created_at >= $2
	`, pubkey, cutoff)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("load profile windowed active days: %w", err)
	}

	engagement := replyReceived + repostReceived + reactionReceived + zapCount
	return postCount, replyCount, engagement, zapVolumeMSats, int(activeDaysRaw), nil
}

func computeProfileTrendingScore(
	window time.Duration,
	nowUnix int64,
	recentActivityAt *int64,
	postCount int64,
	replyCount int64,
	engagementReceived int64,
	zapVolumeMSats int64,
	activeDays int,
) float64 {
	windowSeconds := int64(window / time.Second)
	if windowSeconds <= 0 || recentActivityAt == nil {
		return 0
	}
	ageSeconds := nowUnix - *recentActivityAt
	if ageSeconds < 0 {
		ageSeconds = 0
	}
	if ageSeconds > windowSeconds {
		return 0
	}
	base := float64(postCount)*2.0 +
		float64(replyCount)*1.5 +
		float64(engagementReceived)*2.5 +
		(float64(zapVolumeMSats) / 100000.0) +
		float64(activeDays)*0.75
	decay := 1.0 / (1.0 + (float64(ageSeconds) / float64(windowSeconds)))
	score := base * decay
	if score <= 0 {
		return 0
	}
	return math.Round(score*1000.0) / 1000.0
}

func computeProfileRisingScore(
	trendingScore float64,
	followerCount int64,
	engagementReceived int64,
	postCount int64,
	replyCount int64,
	activeDays int,
) float64 {
	if trendingScore <= 0 {
		return 0
	}
	safeFollowerCount := maxInt64(0, followerCount)
	audiencePenalty := 1.0 + math.Log10(1.0+float64(safeFollowerCount))
	if audiencePenalty <= 0 {
		audiencePenalty = 1.0
	}
	momentum := float64(engagementReceived) + float64(postCount) + float64(replyCount)
	if activeDays > 0 {
		momentum = momentum / float64(activeDays)
	}
	score := (trendingScore + momentum) / audiencePenalty
	if score <= 0 {
		return 0
	}
	return math.Round(score*1000.0) / 1000.0
}

func maxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
