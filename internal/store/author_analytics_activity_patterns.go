package store

import (
	"context"
	"fmt"
	"strings"
)

func (s *PostgresStore) GetAuthorActivityWindowBuckets(
	ctx context.Context,
	pubkey string,
	windowDays int,
) ([]AuthorActivityWindowBucketProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}

	rows, err := s.pool.Query(ctx, `
		WITH calendar AS (
			SELECT d AS day_of_week, h AS hour_of_day
			FROM generate_series(0, 6) AS d
			CROSS JOIN generate_series(0, 23) AS h
		)
		SELECT
			$1 AS pubkey,
			$2 AS window_days,
			c.day_of_week,
			c.hour_of_day,
			COALESCE(w.engagement_received, 0),
			COALESCE(w.reply_received, 0),
			COALESCE(w.reaction_received, 0),
			COALESCE(w.repost_received, 0),
			COALESCE(w.zap_received, 0)
		FROM calendar c
		LEFT JOIN author_activity_windows w
			ON w.pubkey = $1
		   AND w.window_days = $2
		   AND w.day_of_week = c.day_of_week
		   AND w.hour_of_day = c.hour_of_day
		ORDER BY c.day_of_week ASC, c.hour_of_day ASC
	`, pubkey, windowDays)
	if err != nil {
		return nil, fmt.Errorf("get author activity window buckets: %w", err)
	}
	defer rows.Close()

	out := make([]AuthorActivityWindowBucketProjection, 0, 7*24)
	for rows.Next() {
		var row AuthorActivityWindowBucketProjection
		if err := rows.Scan(
			&row.Pubkey,
			&row.WindowDays,
			&row.DayOfWeek,
			&row.HourOfDay,
			&row.EngagementReceived,
			&row.ReplyReceived,
			&row.ReactionReceived,
			&row.RepostReceived,
			&row.ZapReceived,
		); err != nil {
			return nil, fmt.Errorf("scan author activity window bucket row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author activity window bucket rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorPostingPatternBuckets(
	ctx context.Context,
	pubkey string,
	windowDays int,
) ([]AuthorPostingPatternBucketProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}

	rows, err := s.pool.Query(ctx, `
		WITH calendar AS (
			SELECT d AS day_of_week, h AS hour_of_day
			FROM generate_series(0, 6) AS d
			CROSS JOIN generate_series(0, 23) AS h
		)
		SELECT
			$1 AS pubkey,
			$2 AS window_days,
			c.day_of_week,
			c.hour_of_day,
			COALESCE(p.post_count, 0),
			COALESCE(p.note_count, 0),
			COALESCE(p.reply_count, 0)
		FROM calendar c
		LEFT JOIN author_posting_patterns p
			ON p.pubkey = $1
		   AND p.window_days = $2
		   AND p.day_of_week = c.day_of_week
		   AND p.hour_of_day = c.hour_of_day
		ORDER BY c.day_of_week ASC, c.hour_of_day ASC
	`, pubkey, windowDays)
	if err != nil {
		return nil, fmt.Errorf("get author posting pattern buckets: %w", err)
	}
	defer rows.Close()

	out := make([]AuthorPostingPatternBucketProjection, 0, 7*24)
	for rows.Next() {
		var row AuthorPostingPatternBucketProjection
		if err := rows.Scan(
			&row.Pubkey,
			&row.WindowDays,
			&row.DayOfWeek,
			&row.HourOfDay,
			&row.PostCount,
			&row.NoteCount,
			&row.ReplyCount,
		); err != nil {
			return nil, fmt.Errorf("scan author posting pattern bucket row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author posting pattern bucket rows: %w", err)
	}
	return out, nil
}
