package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// SearchEventsByContent returns note-like events filtered by content text.
func (s *Read) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	return s.SearchNotes(ctx, query, "relevant", nil, "", limit, 0)
}

// SearchNotes returns note-like events filtered by content text with minimal sorting/filtering options.
func (s *Read) SearchNotes(
	ctx context.Context,
	query string,
	sort string,
	window *time.Duration,
	language string,
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
	language = normalizeLanguageFilter(language)
	if language == invalidLanguageFilter {
		return nil, fmt.Errorf("unsupported notes language filter: %s", strings.TrimSpace(language))
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
				events.raw_json::text AS raw_text,
				events.created_at,
				events.id,
				ts_rank_cd(
					to_tsvector('simple', coalesce(events.content, '')),
					websearch_to_tsquery('simple', $1)
				) AS rank
			FROM events
			LEFT JOIN note_discovery_stats nds ON nds.event_id = events.id
			WHERE events.kind IN (1, 30023)
			  AND ($3::bigint IS NULL OR events.created_at >= (extract(epoch from now())::bigint - $3::bigint))
			  AND (
				$4::text IS NULL OR (
					CASE
						WHEN $4::text = 'und' THEN nds.primary_language IS NULL
						ELSE nds.primary_language = $4::text
					END
				)
			  )
			  AND (
				to_tsvector('simple', coalesce(events.content, '')) @@ websearch_to_tsquery('simple', $1)
				OR events.content ILIKE '%' || $1 || '%'
			  )
		)
		SELECT raw_text
		FROM ranked
		ORDER BY
			CASE WHEN $2 = 'relevant' THEN rank END DESC,
			created_at DESC,
			id DESC
		LIMIT $5
		OFFSET $6
	`, query, sort, windowSeconds, nullableLanguageFilter(language), limit, offset)
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

const invalidLanguageFilter = "__invalid_language_filter__"

func normalizeLanguageFilter(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return ""
	}
	if value == "und" {
		return value
	}
	if len(value) < 2 || len(value) > 8 {
		return invalidLanguageFilter
	}
	for _, r := range value {
		if r < 'a' || r > 'z' {
			return invalidLanguageFilter
		}
	}
	return value
}

func nullableLanguageFilter(language string) any {
	if language == "" {
		return nil
	}
	return language
}

// SearchProfiles returns latest profile projections matching query.
func (s *Read) SearchProfiles(ctx context.Context, query string, limit int) ([]ProfileProjection, error) {
	return s.SearchProfilesWithOptions(ctx, query, "relevant", limit, 0)
}

// SearchProfilesWithOptions returns latest profile projections matching query with minimal sorting/pagination options.
func (s *Read) SearchProfilesWithOptions(
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
		WITH latest_metadata AS (
			SELECT DISTINCT ON (events.pubkey)
				events.pubkey,
				events.id AS metadata_event_id,
				events.created_at AS metadata_created_at,
				events.content AS profile_text
			FROM events
			LEFT JOIN profiles_latest ON profiles_latest.pubkey = events.pubkey
			WHERE events.kind = 0
			  AND (
				profiles_latest.pubkey IS NULL
				OR events.created_at > profiles_latest.metadata_created_at
				OR (events.created_at = profiles_latest.metadata_created_at AND events.id > profiles_latest.metadata_event_id)
			  )
			ORDER BY events.pubkey, events.created_at DESC, events.id DESC
		),
		candidate_profiles AS (
			SELECT
				profiles_latest.pubkey,
				profiles_latest.metadata_event_id,
				profiles_latest.metadata_created_at,
				profiles_latest.profile_json::text AS profile_text,
				coalesce(profiles_latest.name, '') AS name,
				coalesce(profiles_latest.display_name, '') AS display_name,
				coalesce(profiles_latest.about, '') AS about,
				coalesce(profiles_latest.nip05, '') AS nip05,
				coalesce(profiles_latest.name, '') || ' ' ||
				coalesce(profiles_latest.display_name, '') || ' ' ||
				coalesce(profiles_latest.about, '') || ' ' ||
				coalesce(profiles_latest.nip05, '') AS profile_blob
			FROM profiles_latest
			LEFT JOIN latest_metadata ON latest_metadata.pubkey = profiles_latest.pubkey
			WHERE latest_metadata.pubkey IS NULL
			UNION ALL
			SELECT
				pubkey,
				metadata_event_id,
				metadata_created_at,
				profile_text,
				'' AS name,
				'' AS display_name,
				'' AS about,
				'' AS nip05,
				coalesce(profile_text, '') AS profile_blob
			FROM latest_metadata
		),
		ranked AS (
			SELECT
				pubkey,
				metadata_event_id,
				metadata_created_at,
				profile_text,
				ts_rank_cd(
					to_tsvector(
						'simple',
						coalesce(pubkey, '') || ' ' || coalesce(profile_blob, '')
					),
					websearch_to_tsquery('simple', $1)
				) AS rank
			FROM candidate_profiles
			WHERE
				to_tsvector(
					'simple',
					coalesce(pubkey, '') || ' ' || coalesce(profile_blob, '')
				) @@ websearch_to_tsquery('simple', $1)
				OR pubkey ILIKE '%' || $1 || '%'
				OR coalesce(name, '') ILIKE '%' || $1 || '%'
				OR coalesce(display_name, '') ILIKE '%' || $1 || '%'
				OR coalesce(about, '') ILIKE '%' || $1 || '%'
				OR coalesce(nip05, '') ILIKE '%' || $1 || '%'
				OR coalesce(profile_blob, '') ILIKE '%' || $1 || '%'
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
func (s *Read) SuggestProfiles(ctx context.Context, query string, limit int) ([]ProfileProjection, error) {
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
		WITH latest_metadata AS (
			SELECT DISTINCT ON (events.pubkey)
				events.pubkey,
				events.id AS metadata_event_id,
				events.created_at AS metadata_created_at,
				events.content AS profile_text
			FROM events
			LEFT JOIN profiles_latest ON profiles_latest.pubkey = events.pubkey
			WHERE events.kind = 0
			  AND (
				profiles_latest.pubkey IS NULL
				OR events.created_at > profiles_latest.metadata_created_at
				OR (events.created_at = profiles_latest.metadata_created_at AND events.id > profiles_latest.metadata_event_id)
			  )
			ORDER BY events.pubkey, events.created_at DESC, events.id DESC
		),
		candidate_profiles AS (
			SELECT
				profiles_latest.pubkey,
				profiles_latest.metadata_event_id,
				profiles_latest.metadata_created_at,
				profiles_latest.profile_json::text AS profile_text,
				coalesce(profiles_latest.name, '') AS name,
				coalesce(profiles_latest.display_name, '') AS display_name,
				coalesce(profiles_latest.nip05, '') AS nip05
			FROM profiles_latest
			LEFT JOIN latest_metadata ON latest_metadata.pubkey = profiles_latest.pubkey
			WHERE latest_metadata.pubkey IS NULL
			UNION ALL
			SELECT
				pubkey,
				metadata_event_id,
				metadata_created_at,
				profile_text,
				'' AS name,
				'' AS display_name,
				'' AS nip05
			FROM latest_metadata
		),
		ranked AS (
			SELECT
				pubkey,
				metadata_event_id,
				metadata_created_at,
				profile_text,
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
			FROM candidate_profiles
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
func (s *Read) SuggestHashtags(ctx context.Context, query string, limit int) ([]TrendingHashtag, error) {
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
