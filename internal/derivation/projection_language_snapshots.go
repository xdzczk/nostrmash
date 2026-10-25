package derivation

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/languagelabel"
)

const rankingLanguageLimit = 8

const (
	relaySnapshotLabelHomeWindow24hLang  = "home_window_24h.lang"
	relaySnapshotLabelHomeWindow7dLang   = "home_window_7d.lang"
	relaySnapshotLabelTopHashtags24hLang = "top_hashtags_24h.lang"
	relaySnapshotLabelTopHashtags7dLang  = "top_hashtags_7d.lang"
	relaySnapshotLabelTopDomains24hLang  = "top_domains_24h.lang"
	relaySnapshotLabelTopDomains7dLang   = "top_domains_7d.lang"
)

// computeRankingLanguages is the set of real languages a language view can
// rank. Und and und-uncertain stay out: they are chart buckets, not languages
// a visitor selects. Eight matches the chart the selector is built from, and
// because the chart may also contain those two buckets, the top eight real
// languages cover every selectable code in it.
func computeRankingLanguages(ctx context.Context, tx pgx.Tx, minCreatedAt int64) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT language
		FROM (
			SELECT `+languagelabel.ChartSQL("primary_language", "language_source")+` AS language
			FROM note_discovery_stats
			WHERE created_at >= $1
		) labeled
		WHERE language NOT IN ('und', 'und-uncertain')
		GROUP BY language
		ORDER BY COUNT(*) DESC, language ASC
		LIMIT $2
	`, minCreatedAt, rankingLanguageLimit)
	if err != nil {
		return nil, fmt.Errorf("query ranking languages: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, rankingLanguageLimit)
	for rows.Next() {
		var language string
		if err := rows.Scan(&language); err != nil {
			return nil, fmt.Errorf("scan ranking language: %w", err)
		}
		out = append(out, language)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read ranking languages: %w", err)
	}
	return out, nil
}

type homeWindowLanguageRow struct {
	NoteVolume    int64 `json:"note_volume"`
	ActiveAuthors int64 `json:"active_authors"`
}

func computeHomeWindowByLanguage(ctx context.Context, tx pgx.Tx, minCreatedAt int64, languages []string) (map[string]homeWindowLanguageRow, error) {
	out := map[string]homeWindowLanguageRow{}
	if len(languages) == 0 {
		return out, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT
			primary_language,
			COUNT(*)::bigint,
			COUNT(DISTINCT author_pubkey)::bigint
		FROM note_discovery_stats
		WHERE created_at >= $1
		  AND primary_language = ANY($2::text[])
		GROUP BY primary_language
	`, minCreatedAt, languages)
	if err != nil {
		return nil, fmt.Errorf("query home window by language: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var language string
		var row homeWindowLanguageRow
		if err := rows.Scan(&language, &row.NoteVolume, &row.ActiveAuthors); err != nil {
			return nil, fmt.Errorf("scan home window language: %w", err)
		}
		out[language] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read home window languages: %w", err)
	}
	return out, nil
}

func computeTopHashtagsByLanguage(ctx context.Context, tx pgx.Tx, minCreatedAt int64, languages []string, limit int) (map[string][]hashtagRow, error) {
	out := map[string][]hashtagRow{}
	if len(languages) == 0 {
		return out, nil
	}
	if limit <= 0 {
		limit = 50
	}
	query := fmt.Sprintf(`
		SELECT language, hashtag, event_count, unique_authors
		FROM (
			SELECT
				nds.primary_language AS language,
				h.hashtag,
				COUNT(*)::bigint AS event_count,
				COUNT(DISTINCT h.author_pubkey)::bigint AS unique_authors,
				ROW_NUMBER() OVER (
					PARTITION BY nds.primary_language
					ORDER BY
						COUNT(DISTINCT h.author_pubkey) DESC,
						(COUNT(DISTINCT h.author_pubkey))::double precision / GREATEST(COUNT(*), 1) DESC,
						COUNT(*) DESC,
						h.hashtag ASC
				) AS rn
			FROM event_hashtags h
			INNER JOIN note_discovery_stats nds ON nds.event_id = h.event_id
			%s
			WHERE h.created_at >= $1
			  AND nds.primary_language = ANY($2::text[])
			  AND h.hashtag !~ '^[0-9]+$'
			GROUP BY nds.primary_language, h.hashtag
		) ranked
		WHERE rn <= $3
		ORDER BY language ASC, rn ASC
	`, fmt.Sprintf(trustedAuthorJoinClause, "h"))
	rows, err := tx.Query(ctx, query, minCreatedAt, languages, limit)
	if err != nil {
		return nil, fmt.Errorf("query top hashtags by language: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var language string
		var row hashtagRow
		if err := rows.Scan(&language, &row.Hashtag, &row.EventCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan hashtag language row: %w", err)
		}
		out[language] = append(out[language], row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read hashtag language rows: %w", err)
	}
	return out, nil
}

func computeTopDomainsByLanguage(
	ctx context.Context,
	tx pgx.Tx,
	windowFloor int64,
	cutoff24h int64,
	cutoff7d int64,
	languages []string,
	limit int,
) (map[string][]domainRow, error) {
	out := map[string][]domainRow{}
	if len(languages) == 0 {
		return out, nil
	}
	if limit <= 0 {
		limit = 50
	}
	query := fmt.Sprintf(`
		SELECT
			language,
			canonical_domain,
			latest_event_at,
			links_24h,
			notes_24h,
			authors_24h,
			links_7d,
			notes_7d,
			authors_7d
		FROM (
			SELECT
				nds.primary_language AS language,
				u.canonical_domain,
				MAX(u.created_at) AS latest_event_at,
				COUNT(*) FILTER (WHERE u.created_at >= $1) AS links_24h,
				COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $1) AS notes_24h,
				COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $1) AS authors_24h,
				COUNT(*) FILTER (WHERE u.created_at >= $2) AS links_7d,
				COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $2) AS notes_7d,
				COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $2) AS authors_7d,
				ROW_NUMBER() OVER (
					PARTITION BY nds.primary_language
					ORDER BY
						COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $3) DESC,
						(COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $3))::double precision /
							GREATEST(COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $3), 1) DESC,
						COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $3) DESC,
						COUNT(*) FILTER (WHERE u.created_at >= $3) DESC,
						u.canonical_domain ASC
				) AS rn
			FROM event_urls u
			INNER JOIN note_discovery_stats nds ON nds.event_id = u.event_id
			%s
			WHERE u.created_at >= $3
			  AND nds.primary_language = ANY($4::text[])
			  AND %s
			GROUP BY nds.primary_language, u.canonical_domain
		) ranked
		WHERE rn <= $5
		ORDER BY language ASC, rn ASC
	`, fmt.Sprintf(trustedAuthorJoinClause, "u"), domainMediaURLFilterClause)
	rows, err := tx.Query(ctx, query, cutoff24h, cutoff7d, windowFloor, languages, limit)
	if err != nil {
		return nil, fmt.Errorf("query top domains by language: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var language string
		var row domainRow
		if err := rows.Scan(
			&language,
			&row.Domain,
			&row.LatestEventAt,
			&row.Activity24h.LinkCount,
			&row.Activity24h.NoteCount,
			&row.Activity24h.UniqueAuthors,
			&row.Activity7d.LinkCount,
			&row.Activity7d.NoteCount,
			&row.Activity7d.UniqueAuthors,
		); err != nil {
			return nil, fmt.Errorf("scan domain language row: %w", err)
		}
		out[language] = append(out[language], row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read domain language rows: %w", err)
	}
	return out, nil
}
