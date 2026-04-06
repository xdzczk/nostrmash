package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// SearchEventsByContent returns note-like events filtered by content text.
func (s *PostgresStore) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return []json.RawMessage{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
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
			  AND (
				to_tsvector('simple', coalesce(content, '')) @@ websearch_to_tsquery('simple', $1)
				OR content ILIKE '%' || $1 || '%'
			  )
		)
		SELECT raw_text
		FROM ranked
		ORDER BY rank DESC, created_at DESC, id DESC
		LIMIT $2
	`, query, limit)
	if err != nil {
		return nil, fmt.Errorf("search events by content: %w", err)
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
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return []ProfileProjection{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
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
	`, query, limit)
	if err != nil {
		return nil, fmt.Errorf("search profiles: %w", err)
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
