package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (s *PostgresStore) SearchDocuments(ctx context.Context, query string, limit int) ([]SearchDocumentProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return []SearchDocumentProjection{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		WITH params AS (
			SELECT lower(trim($1)) AS q
		),
		ranked AS (
			SELECT
				doc.entity_type,
				doc.entity_id,
				doc.title,
				doc.body,
				doc.aliases,
				doc.identity_tokens,
				doc.freshness,
				doc.popularity,
				doc.trust_score,
				(
					CASE
						WHEN lower(doc.entity_id) = params.q THEN 120
						WHEN EXISTS (
							SELECT 1
							FROM unnest(doc.aliases) AS alias
							WHERE lower(alias) = params.q
						) THEN 95
						WHEN lower(doc.entity_id) LIKE params.q || '%' THEN 80
						WHEN lower(coalesce(doc.title, '')) LIKE params.q || '%' THEN 70
						WHEN lower(coalesce(doc.title, '')) = params.q THEN 90
						ELSE 0
					END
					+ COALESCE(ts_rank_cd(doc.search_tsv, websearch_to_tsquery('simple', params.q)) * 25, 0)
					+ CASE
						WHEN lower(coalesce(doc.body, '')) LIKE '%' || params.q || '%' THEN 5
						ELSE 0
					END
					+ LEAST(coalesce(doc.popularity, 0) / 1000.0, 8)
				) AS rank_score
			FROM search_documents doc
			CROSS JOIN params
			WHERE
				doc.search_tsv @@ websearch_to_tsquery('simple', params.q)
				OR lower(doc.entity_id) LIKE '%' || params.q || '%'
				OR lower(coalesce(doc.title, '')) LIKE '%' || params.q || '%'
				OR EXISTS (
					SELECT 1
					FROM unnest(doc.aliases) AS alias
					WHERE lower(alias) LIKE '%' || params.q || '%'
				)
				OR EXISTS (
					SELECT 1
					FROM unnest(doc.identity_tokens) AS token
					WHERE lower(token) LIKE '%' || params.q || '%'
				)
		)
		SELECT
			entity_type,
			entity_id,
			title,
			body,
			aliases,
			identity_tokens,
			freshness,
			popularity,
			trust_score,
			rank_score
		FROM ranked
		ORDER BY rank_score DESC, freshness DESC, popularity DESC, entity_id ASC
		LIMIT $2
	`, query, limit)
	if err != nil {
		return nil, fmt.Errorf("search documents: %w", err)
	}
	defer rows.Close()

	out := make([]SearchDocumentProjection, 0, limit)
	for rows.Next() {
		var (
			row       SearchDocumentProjection
			freshness time.Time
		)
		if err := rows.Scan(
			&row.EntityType,
			&row.EntityID,
			&row.Title,
			&row.Body,
			&row.Aliases,
			&row.IdentityTokens,
			&freshness,
			&row.Popularity,
			&row.TrustScore,
			&row.Score,
		); err != nil {
			return nil, fmt.Errorf("scan search document row: %w", err)
		}
		row.Freshness = freshness.UTC()
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read search document rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) RebuildSearchDocuments(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO search_documents (
			entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
		)
		SELECT
			'profile',
			profile.pubkey,
			coalesce(nullif(profile.display_name, ''), nullif(profile.name, ''), profile.pubkey),
			coalesce(profile.about, ''),
			array_remove(array[profile.pubkey, profile.name, profile.display_name, profile.nip05], NULL),
			array_remove(array[profile.pubkey, profile.nip05], NULL),
			now(),
			coalesce((SELECT (coalesce(stats.follower_count, 0) + coalesce(stats.note_count, 0))::double precision
			          FROM profile_public_stats stats
			          WHERE stats.pubkey = profile.pubkey), 0),
			NULL,
			now()
		FROM profiles_latest profile
		ON CONFLICT (entity_type, entity_id) DO UPDATE
		SET title = EXCLUDED.title,
		    body = EXCLUDED.body,
		    aliases = EXCLUDED.aliases,
		    identity_tokens = EXCLUDED.identity_tokens,
		    freshness = EXCLUDED.freshness,
		    popularity = EXCLUDED.popularity,
		    trust_score = EXCLUDED.trust_score,
		    updated_at = now()
	`)
	if err != nil {
		return fmt.Errorf("rebuild profile search documents: %w", err)
	}
	return nil
}
