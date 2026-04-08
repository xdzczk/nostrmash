package store

import (
	"context"
	"fmt"
	"time"
)

func (s *PostgresStore) GetTrendingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
	return s.getProfileDiscoveryRows(ctx, window, limit, offset, false)
}

func (s *PostgresStore) GetRisingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
	return s.getProfileDiscoveryRows(ctx, window, limit, offset, true)
}

func (s *PostgresStore) getProfileDiscoveryRows(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	rising bool,
) ([]TrendingProfile, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	scoreColumn, windowDuration, err := resolveTrendingWindow(window)
	if err != nil {
		return nil, err
	}
	if rising {
		switch scoreColumn {
		case "score_24h":
			scoreColumn = "rising_score_24h"
		case "score_7d":
			scoreColumn = "rising_score_7d"
		}
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	minCreatedAt := time.Now().UTC().Add(-windowDuration).Unix()
	query := fmt.Sprintf(`
		SELECT
			pubkey,
			%s AS score,
			recent_post_count,
			recent_reply_count,
			recent_engagement_received,
			recent_zap_volume_msats,
			recent_active_days,
			recent_activity_at
		FROM profile_discovery_stats
		WHERE recent_activity_at IS NOT NULL
		  AND recent_activity_at >= $1
		  AND %s > 0
		ORDER BY score DESC, recent_engagement_received DESC, recent_activity_at DESC, pubkey ASC
		LIMIT $2 OFFSET $3
	`, scoreColumn, scoreColumn)
	rows, err := s.pool.Query(ctx, query, minCreatedAt, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get profile discovery rows: %w", err)
	}
	defer rows.Close()

	out := make([]TrendingProfile, 0, limit)
	for rows.Next() {
		var row TrendingProfile
		if err := rows.Scan(
			&row.Pubkey,
			&row.Score,
			&row.RecentPostCount,
			&row.RecentReplyCount,
			&row.RecentEngagementReceived,
			&row.RecentZapVolumeMSats,
			&row.RecentActiveDays,
			&row.RecentActivityAt,
		); err != nil {
			return nil, fmt.Errorf("scan profile discovery row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read profile discovery rows: %w", err)
	}
	return out, nil
}
