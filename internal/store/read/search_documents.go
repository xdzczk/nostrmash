package read

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (s *Read) SearchDocuments(ctx context.Context, query string, limit int) ([]SearchDocumentProjection, error) {
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

	// Each branch is bounded and indexable. OR-ing a leading-wildcard LIKE
	// with the tsvector match made Postgres scan search_documents, and that
	// fallback then exceeded the search deadline. Alias and identity
	// substring matches stay on Meilisearch; this path keeps full-text hits
	// plus identifier and title prefixes.
	rows, err := s.pool.Query(ctx, `
		WITH params AS (
			SELECT lower(trim($1)) AS q
		),
		hit AS (
			(
				SELECT
					doc.entity_type, doc.entity_id, doc.title, doc.body, doc.aliases,
					doc.identity_tokens, doc.freshness, doc.popularity, doc.trust_score, doc.search_tsv
				FROM search_documents doc
				CROSS JOIN params
				WHERE params.q <> ''
				  AND doc.search_tsv @@ websearch_to_tsquery('simple', params.q)
				ORDER BY doc.popularity DESC, doc.freshness DESC
				LIMIT 200
			)
			UNION
			(
				SELECT
					doc.entity_type, doc.entity_id, doc.title, doc.body, doc.aliases,
					doc.identity_tokens, doc.freshness, doc.popularity, doc.trust_score, doc.search_tsv
				FROM search_documents doc
				CROSS JOIN params
				WHERE params.q <> ''
				  AND lower(doc.entity_id) LIKE params.q || '%'
				ORDER BY doc.popularity DESC, doc.freshness DESC
				LIMIT 50
			)
			UNION
			(
				SELECT
					doc.entity_type, doc.entity_id, doc.title, doc.body, doc.aliases,
					doc.identity_tokens, doc.freshness, doc.popularity, doc.trust_score, doc.search_tsv
				FROM search_documents doc
				CROSS JOIN params
				WHERE params.q <> ''
				  AND left(lower(doc.title), 200) LIKE params.q || '%'
				ORDER BY doc.popularity DESC, doc.freshness DESC
				LIMIT 50
			)
		)
		SELECT
			hit.entity_type,
			hit.entity_id,
			hit.title,
			hit.body,
			hit.aliases,
			hit.identity_tokens,
			hit.freshness,
			hit.popularity,
			hit.trust_score,
			(
				CASE
					WHEN lower(hit.entity_id) = params.q THEN 120
					WHEN EXISTS (
						SELECT 1
						FROM unnest(hit.aliases) AS alias
						WHERE lower(alias) = params.q
					) THEN 95
					WHEN lower(coalesce(hit.title, '')) = params.q THEN 90
					WHEN lower(hit.entity_id) LIKE params.q || '%' THEN 80
					WHEN lower(coalesce(hit.title, '')) LIKE params.q || '%' THEN 70
					ELSE 0
				END
				+ COALESCE(ts_rank_cd(hit.search_tsv, websearch_to_tsquery('simple', params.q)) * 25, 0)
				+ LEAST(coalesce(hit.popularity, 0) / 1000.0, 8)
			) AS rank_score
		FROM hit
		CROSS JOIN params
		ORDER BY rank_score DESC, hit.freshness DESC, hit.popularity DESC, hit.entity_id ASC
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

func (s *Read) RebuildSearchDocuments(ctx context.Context) error {
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
