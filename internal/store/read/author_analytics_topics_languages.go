package read

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (s *Read) GetAuthorTopicStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]AuthorTopicStatsProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, window_days, hashtag, usage_count, active_days
		FROM author_topic_stats
		WHERE pubkey = $1
		  AND window_days = $2
		ORDER BY usage_count DESC, hashtag ASC
		LIMIT $3
	`, pubkey, windowDays, limit)
	if err != nil {
		return nil, fmt.Errorf("get author topic stats: %w", err)
	}
	defer rows.Close()

	out := make([]AuthorTopicStatsProjection, 0, limit)
	for rows.Next() {
		var row AuthorTopicStatsProjection
		if err := rows.Scan(
			&row.Pubkey,
			&row.WindowDays,
			&row.Hashtag,
			&row.UsageCount,
			&row.ActiveDays,
		); err != nil {
			return nil, fmt.Errorf("scan author topic stats row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author topic stats rows: %w", err)
	}
	return out, nil
}

func (s *Read) GetAuthorTopLanguages(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]LanguageSummary, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if windowDays <= 0 {
		windowDays = 30
	}
	if limit <= 0 {
		limit = 8
	}
	if limit > 20 {
		limit = 20
	}
	cutoff := time.Now().UTC().Unix() - int64(windowDays*24*60*60)
	rows, err := s.pool.Query(ctx, `
		SELECT
			COALESCE(primary_language, 'und') AS language,
			COUNT(*)::bigint AS count_value
		FROM note_discovery_stats
		WHERE author_pubkey = $1
		  AND created_at >= $2
		GROUP BY COALESCE(primary_language, 'und')
		ORDER BY count_value DESC, language ASC
		LIMIT $3
	`, pubkey, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("get author top languages: %w", err)
	}
	defer rows.Close()
	out := make([]LanguageSummary, 0, limit)
	for rows.Next() {
		var row LanguageSummary
		if err := rows.Scan(&row.Language, &row.Count); err != nil {
			return nil, fmt.Errorf("scan author top language row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author top language rows: %w", err)
	}
	return out, nil
}
