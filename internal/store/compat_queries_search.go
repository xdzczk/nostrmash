package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// SearchEventsByContent returns note-like events filtered by content text.
func (s *PostgresStore) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	return s.SearchNotes(ctx, query, "relevant", nil, limit, 0)
}

// SearchNotes returns note-like events filtered by content text with minimal sorting/filtering options.
func (s *PostgresStore) SearchNotes(
	ctx context.Context,
	query string,
	sort string,
	window *time.Duration,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return []json.RawMessage{}, nil
	}
	sort = strings.ToLower(strings.TrimSpace(sort))
	if sort == "" {
		sort = "relevant"
	}
	switch sort {
	case "relevant", "latest":
	default:
		return nil, fmt.Errorf("unsupported notes sort: %s", sort)
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset must be non-negative")
	}
	if offset > 5000 {
		return nil, fmt.Errorf("offset exceeds maximum allowed value")
	}
	var windowSeconds any
	if window != nil {
		seconds := int64(window.Seconds())
		if seconds > 0 {
			windowSeconds = seconds
		}
	}
	rows, err := s.pool.Query(ctx, `
		WITH ranked AS (
			SELECT
				raw_json::text AS raw_text,
				created_at,
				id,
				ts_rank_cd(
					to_tsvector('simple', coalesce(content, '')),
					websearch_to_tsquery('simple', $1)
				) AS rank
			FROM events
			WHERE kind = 1
			  AND ($3::bigint IS NULL OR created_at >= (extract(epoch from now())::bigint - $3::bigint))
			  AND (
				to_tsvector('simple', coalesce(content, '')) @@ websearch_to_tsquery('simple', $1)
				OR content ILIKE '%' || $1 || '%'
			  )
		)
		SELECT raw_text
		FROM ranked
		ORDER BY
			CASE WHEN $2 = 'relevant' THEN rank END DESC,
			created_at DESC,
			id DESC
		LIMIT $4
		OFFSET $5
	`, query, sort, windowSeconds, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("search notes: %w", err)
	}
	defer rows.Close()

	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan searched event row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read searched event rows: %w", err)
	}
	return out, nil
}

// SearchProfiles returns latest profile projections matching query.
func (s *PostgresStore) SearchProfiles(ctx context.Context, query string, limit int) ([]ProfileProjection, error) {
	return s.SearchProfilesWithOptions(ctx, query, "relevant", limit, 0)
}

// SearchProfilesWithOptions returns latest profile projections matching query with minimal sorting/pagination options.
func (s *PostgresStore) SearchProfilesWithOptions(
	ctx context.Context,
	query string,
	sort string,
	limit int,
	offset int,
) ([]ProfileProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return []ProfileProjection{}, nil
	}
	sort = strings.ToLower(strings.TrimSpace(sort))
	if sort == "" {
		sort = "relevant"
	}
	if sort != "relevant" {
		return nil, fmt.Errorf("unsupported profile sort: %s", sort)
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset must be non-negative")
	}
	if offset > 5000 {
		return nil, fmt.Errorf("offset exceeds maximum allowed value")
	}

	rows, err := s.pool.Query(ctx, `
		WITH ranked AS (
			SELECT
				pubkey,
				metadata_event_id,
				metadata_created_at,
				profile_json::text AS profile_text,
				ts_rank_cd(
					to_tsvector(
						'simple',
						coalesce(pubkey, '') || ' ' ||
						coalesce(name, '') || ' ' ||
						coalesce(display_name, '') || ' ' ||
						coalesce(about, '') || ' ' ||
						coalesce(nip05, '')
					),
					websearch_to_tsquery('simple', $1)
				) AS rank
			FROM profiles_latest
			WHERE
				to_tsvector(
					'simple',
					coalesce(pubkey, '') || ' ' ||
					coalesce(name, '') || ' ' ||
					coalesce(display_name, '') || ' ' ||
					coalesce(about, '') || ' ' ||
					coalesce(nip05, '')
				) @@ websearch_to_tsquery('simple', $1)
				OR pubkey ILIKE '%' || $1 || '%'
				OR coalesce(name, '') ILIKE '%' || $1 || '%'
				OR coalesce(display_name, '') ILIKE '%' || $1 || '%'
				OR coalesce(about, '') ILIKE '%' || $1 || '%'
				OR coalesce(nip05, '') ILIKE '%' || $1 || '%'
		)
		SELECT pubkey, metadata_event_id, metadata_created_at, profile_text
		FROM ranked
		ORDER BY rank DESC, metadata_created_at DESC, metadata_event_id DESC
		LIMIT $2
		OFFSET $3
	`, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("search profiles with options: %w", err)
	}
	defer rows.Close()

	out := make([]ProfileProjection, 0, limit)
	for rows.Next() {
		var row ProfileProjection
		var profileText string
		if err := rows.Scan(&row.Pubkey, &row.MetadataEventID, &row.MetadataCreatedAt, &profileText); err != nil {
			return nil, fmt.Errorf("scan searched profile row: %w", err)
		}
		row.ProfileJSON = json.RawMessage(profileText)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read searched profile rows: %w", err)
	}
	return out, nil
}

// SuggestProfiles returns lightweight profile suggestions for typeahead search.
func (s *PostgresStore) SuggestProfiles(ctx context.Context, query string, limit int) ([]ProfileProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return []ProfileProjection{}, nil
	}
	query = strings.TrimPrefix(strings.TrimPrefix(query, "@"), "#")
	query = strings.TrimSpace(query)
	if query == "" {
		return []ProfileProjection{}, nil
	}
	if limit <= 0 {
		limit = 8
	}
	if limit > 20 {
		limit = 20
	}

	rows, err := s.pool.Query(ctx, `
		WITH ranked AS (
			SELECT
				pubkey,
				metadata_event_id,
				metadata_created_at,
				profile_json::text AS profile_text,
				CASE
					WHEN pubkey ILIKE $1 || '%' THEN 4
					WHEN coalesce(name, '') ILIKE $1 || '%' THEN 3
					WHEN coalesce(display_name, '') ILIKE $1 || '%' THEN 3
					WHEN coalesce(nip05, '') ILIKE $1 || '%' THEN 2
					ELSE 0
				END AS prefix_score,
				GREATEST(
					similarity(pubkey, $1),
					similarity(coalesce(name, ''), $1),
					similarity(coalesce(display_name, ''), $1),
					similarity(coalesce(nip05, ''), $1)
				) AS sim
			FROM profiles_latest
			WHERE
				pubkey ILIKE '%' || $1 || '%'
				OR coalesce(name, '') ILIKE '%' || $1 || '%'
				OR coalesce(display_name, '') ILIKE '%' || $1 || '%'
				OR coalesce(nip05, '') ILIKE '%' || $1 || '%'
		)
		SELECT pubkey, metadata_event_id, metadata_created_at, profile_text
		FROM ranked
		ORDER BY prefix_score DESC, sim DESC, metadata_created_at DESC, metadata_event_id DESC
		LIMIT $2
	`, query, limit)
	if err != nil {
		return nil, fmt.Errorf("suggest profiles: %w", err)
	}
	defer rows.Close()

	out := make([]ProfileProjection, 0, limit)
	for rows.Next() {
		var row ProfileProjection
		var profileText string
		if err := rows.Scan(&row.Pubkey, &row.MetadataEventID, &row.MetadataCreatedAt, &profileText); err != nil {
			return nil, fmt.Errorf("scan suggested profile row: %w", err)
		}
		row.ProfileJSON = json.RawMessage(profileText)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read suggested profile rows: %w", err)
	}
	return out, nil
}

// SuggestHashtags returns lightweight hashtag suggestions for typeahead search.
func (s *PostgresStore) SuggestHashtags(ctx context.Context, query string, limit int) ([]TrendingHashtag, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	query = strings.ToLower(strings.TrimSpace(query))
	query = strings.TrimPrefix(strings.TrimPrefix(query, "#"), "@")
	query = strings.TrimSpace(query)
	if query == "" {
		return []TrendingHashtag{}, nil
	}
	if limit <= 0 {
		limit = 8
	}
	if limit > 20 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			hashtag,
			COUNT(*) AS event_count,
			COUNT(DISTINCT author_pubkey) AS unique_authors
		FROM event_hashtags
		WHERE hashtag LIKE $1 || '%'
		GROUP BY hashtag
		ORDER BY event_count DESC, unique_authors DESC, hashtag ASC
		LIMIT $2
	`, query, limit)
	if err != nil {
		return nil, fmt.Errorf("suggest hashtags: %w", err)
	}
	defer rows.Close()

	out := make([]TrendingHashtag, 0, limit)
	for rows.Next() {
		var row TrendingHashtag
		if err := rows.Scan(&row.Hashtag, &row.EventCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan suggested hashtag row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read suggested hashtag rows: %w", err)
	}
	return out, nil
}
