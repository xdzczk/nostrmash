package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (s *PostgresStore) GetTrendingNotes(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	scoreColumn, windowDuration, err := resolveTrendingWindow(window)
	if err != nil {
		return nil, err
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
			s.event_id,
			s.author_pubkey,
			s.created_at,
			e.content,
			COALESCE(s.primary_language, 'und') AS language,
			s.reply_count,
			s.repost_count,
			s.reaction_count,
			s.zap_count,
			s.zap_msats,
			%s AS score
		FROM note_discovery_stats s
		JOIN events e ON e.id = s.event_id
		WHERE s.created_at >= $1
		ORDER BY score DESC, s.created_at DESC, s.event_id ASC
		LIMIT $2 OFFSET $3
	`, scoreColumn)
	rows, err := s.pool.Query(ctx, query, minCreatedAt, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get trending notes: %w", err)
	}
	defer rows.Close()

	out := make([]TrendingNote, 0, limit)
	for rows.Next() {
		var row TrendingNote
		if err := rows.Scan(
			&row.EventID,
			&row.AuthorPubkey,
			&row.CreatedAt,
			&row.Content,
			&row.Language,
			&row.ReplyCount,
			&row.RepostCount,
			&row.ReactionCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.Score,
		); err != nil {
			return nil, fmt.Errorf("scan trending note row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trending note rows: %w", err)
	}
	return out, nil
}

func resolveTrendingWindow(window time.Duration) (string, time.Duration, error) {
	switch window {
	case 24 * time.Hour:
		return "score_24h", 24 * time.Hour, nil
	case 7 * 24 * time.Hour:
		return "score_7d", 7 * 24 * time.Hour, nil
	default:
		return "", 0, fmt.Errorf("unsupported trending window: %s", strings.TrimSpace(window.String()))
	}
}
