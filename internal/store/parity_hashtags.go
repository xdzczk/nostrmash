package store

import (
	"context"
	"fmt"
	"time"
)

func (s *PostgresStore) GetTrendingHashtags(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingHashtag, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if window <= 0 {
		return nil, fmt.Errorf("window must be positive")
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

	minCreatedAt := time.Now().UTC().Add(-window).Unix()
	rows, err := s.pool.Query(ctx, `
		SELECT
			hashtag,
			COUNT(*) AS event_count,
			COUNT(DISTINCT author_pubkey) AS unique_authors
		FROM event_hashtags
		WHERE created_at >= $1
		GROUP BY hashtag
		ORDER BY event_count DESC, unique_authors DESC, hashtag ASC
		LIMIT $2 OFFSET $3
	`, minCreatedAt, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get trending hashtags: %w", err)
	}
	defer rows.Close()

	out := make([]TrendingHashtag, 0, limit)
	for rows.Next() {
		var row TrendingHashtag
		if err := rows.Scan(&row.Hashtag, &row.EventCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan trending hashtag row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trending hashtag rows: %w", err)
	}
	return out, nil
}
