package store

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var storeHashtagPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

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
		ORDER BY
			unique_authors DESC,
			(COUNT(DISTINCT author_pubkey))::double precision / GREATEST(COUNT(*), 1) DESC,
			event_count DESC,
			hashtag ASC
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

func (s *PostgresStore) GetHashtagSummary(ctx context.Context, hashtag string) (HashtagSummary, error) {
	if s == nil || s.pool == nil {
		return HashtagSummary{}, fmt.Errorf("store is not initialized")
	}
	normalized := normalizeStoreHashtag(hashtag)
	if normalized == "" {
		return HashtagSummary{}, ErrNotFound
	}

	var summary HashtagSummary
	var latestCreatedAt *int64
	nowUnix := time.Now().UTC().Unix()
	day := int64((24 * time.Hour) / time.Second)
	if err := s.pool.QueryRow(ctx, `
		SELECT
			MAX(created_at),
			COUNT(*) FILTER (WHERE created_at >= $2),
			COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $2),
			COUNT(*) FILTER (WHERE created_at >= $3),
			COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $3),
			COUNT(*) FILTER (WHERE created_at >= $4),
			COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $4),
			COUNT(*),
			COUNT(DISTINCT author_pubkey)
		FROM event_hashtags
		WHERE hashtag = $1
	`, normalized, nowUnix-day, nowUnix-(7*day), nowUnix-(30*day)).Scan(
		&latestCreatedAt,
		&summary.Activity.Last24h.EventCount,
		&summary.Activity.Last24h.UniqueAuthors,
		&summary.Activity.Last7d.EventCount,
		&summary.Activity.Last7d.UniqueAuthors,
		&summary.Activity.Last30d.EventCount,
		&summary.Activity.Last30d.UniqueAuthors,
		&summary.Activity.All.EventCount,
		&summary.Activity.All.UniqueAuthors,
	); err != nil {
		return HashtagSummary{}, fmt.Errorf("get hashtag summary: %w", err)
	}
	if summary.Activity.All.EventCount == 0 {
		return HashtagSummary{}, ErrNotFound
	}
	summary.Hashtag = normalized
	summary.LatestEventAt = latestCreatedAt
	return summary, nil
}

func (s *PostgresStore) GetHashtagNotes(
	ctx context.Context,
	hashtag string,
	sort string,
	window string,
	limit int,
	offset int,
) ([]TrendingNote, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	normalized := normalizeStoreHashtag(hashtag)
	if normalized == "" {
		return nil, ErrNotFound
	}
	exists, err := s.hasHashtag(ctx, normalized)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	sort = strings.ToLower(strings.TrimSpace(sort))
	if sort == "" {
		sort = "latest"
	}
	if sort != "latest" && sort != "top" {
		return nil, fmt.Errorf("unsupported hashtag notes sort: %s", sort)
	}
	window = strings.ToLower(strings.TrimSpace(window))
	if window == "" {
		window = "24h"
	}
	filterClause, scoreExpr, err := resolveHashtagNotesWindow(window)
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

	orderBy := "s.created_at DESC, s.event_id DESC"
	if sort == "top" {
		orderBy = fmt.Sprintf("%s DESC, s.created_at DESC, s.event_id DESC", scoreExpr)
	}
	query := fmt.Sprintf(`
		SELECT
			s.event_id,
			s.author_pubkey,
			s.created_at,
			e.content,
			s.reply_count,
			s.repost_count,
			s.reaction_count,
			s.zap_count,
			s.zap_msats,
			%s AS score
		FROM event_hashtags h
		JOIN note_discovery_stats s ON s.event_id = h.event_id
		JOIN events e ON e.id = s.event_id
		WHERE h.hashtag = $1
		  AND %s
		ORDER BY %s
		LIMIT $2 OFFSET $3
	`, scoreExpr, filterClause, orderBy)

	rows, err := s.pool.Query(ctx, query, normalized, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get hashtag notes: %w", err)
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
			&row.ReplyCount,
			&row.RepostCount,
			&row.ReactionCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.Score,
		); err != nil {
			return nil, fmt.Errorf("scan hashtag note row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read hashtag note rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetRelatedHashtags(
	ctx context.Context,
	hashtag string,
	limit int,
) ([]RelatedHashtag, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	normalized := normalizeStoreHashtag(hashtag)
	if normalized == "" {
		return nil, ErrNotFound
	}
	exists, err := s.hasHashtag(ctx, normalized)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}

	rows, err := s.pool.Query(ctx, `
		WITH seed AS (
			SELECT event_id
			FROM event_hashtags
			WHERE hashtag = $1
			  AND created_at >= extract(epoch from now())::bigint - (30 * 24 * 60 * 60)
			ORDER BY created_at DESC
			LIMIT 500
		)
		SELECT
			h.hashtag,
			COUNT(*) AS co_occurrence_count,
			COUNT(DISTINCT h.author_pubkey) AS co_occurrence_authors
		FROM event_hashtags h
		JOIN seed s ON s.event_id = h.event_id
		WHERE h.hashtag <> $1
		GROUP BY h.hashtag
		ORDER BY co_occurrence_count DESC, co_occurrence_authors DESC, h.hashtag ASC
		LIMIT $2
	`, normalized, limit)
	if err != nil {
		return nil, fmt.Errorf("get related hashtags: %w", err)
	}
	defer rows.Close()

	out := make([]RelatedHashtag, 0, limit)
	for rows.Next() {
		var row RelatedHashtag
		if err := rows.Scan(&row.Hashtag, &row.CoOccurrenceCount, &row.CoOccurrenceAuthors); err != nil {
			return nil, fmt.Errorf("scan related hashtag row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read related hashtag rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) hasHashtag(ctx context.Context, hashtag string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM event_hashtags WHERE hashtag = $1)`, hashtag).Scan(&exists); err != nil {
		return false, fmt.Errorf("check hashtag existence: %w", err)
	}
	return exists, nil
}

func normalizeStoreHashtag(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.TrimPrefix(normalized, "#")
	normalized = strings.TrimSpace(normalized)
	if normalized == "" || !storeHashtagPattern.MatchString(normalized) {
		return ""
	}
	return normalized
}

func resolveHashtagNotesWindow(window string) (string, string, error) {
	switch window {
	case "24h":
		return "s.created_at >= extract(epoch from now())::bigint - (24 * 60 * 60)", "s.score_24h", nil
	case "7d":
		return "s.created_at >= extract(epoch from now())::bigint - (7 * 24 * 60 * 60)", "s.score_7d", nil
	case "30d":
		return "s.created_at >= extract(epoch from now())::bigint - (30 * 24 * 60 * 60)",
			"((s.reply_count * 3.0) + (s.repost_count * 2.0) + s.reaction_count + (s.zap_count * 2.0) + (s.zap_msats / 100000.0)) / (1.0 + ((extract(epoch from now())::bigint - s.created_at) / (30.0 * 24 * 60 * 60)))",
			nil
	case "all":
		return "TRUE", "((s.reply_count * 3.0) + (s.repost_count * 2.0) + s.reaction_count + (s.zap_count * 2.0) + (s.zap_msats / 100000.0))", nil
	default:
		return "", "", fmt.Errorf("unsupported hashtag notes window: %s", window)
	}
}
