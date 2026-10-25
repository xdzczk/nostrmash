package derivation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func resolveNoteLanguageTx(
	ctx context.Context,
	tx pgx.Tx,
	noteID string,
	authorPubkey string,
	content string,
	nowUnix int64,
	existingVersion *int32,
	existingLang *string,
	existingConf *float64,
	existingSource *string,
) (*string, *float64, string, error) {
	cfg := readLanguageDetectionConfig()
	if languageDecisionSettled(existingVersion, existingSource, existingLang) {
		return existingLang, existingConf, *existingSource, nil
	}
	if !cfg.Enabled {
		return nil, nil, "", nil
	}
	if languageDecisionRetryThread(existingVersion, existingSource) {
		threadLang, err := lookupThreadContentLanguage(ctx, tx, noteID)
		if err != nil {
			return nil, nil, "", err
		}
		if threadLang != nil {
			return threadLang, floatPtr(languageInferredConfidence), languageSourceThread, nil
		}
		return nil, nil, *existingSource, nil
	}

	tags, err := loadNoteTagsTx(ctx, tx, noteID)
	if err != nil {
		return nil, nil, "", err
	}
	tag, tagOK := explicitNoteLanguage(tags)
	cleaned := cleanNoteTextForLanguage(content)
	letters := letterCount(cleaned)
	var contentLang *string
	var contentConf *float64
	if !tagOK && letters >= cfg.MinChars {
		if lang, conf, ok := detectCleanedLanguage(cleaned, cfg); ok {
			contentLang = lang
			contentConf = conf
		}
	}
	var threadLang *string
	var authorLang *string
	if !tagOK && contentLang == nil {
		threadLang, err = lookupThreadContentLanguage(ctx, tx, noteID)
		if err != nil {
			return nil, nil, "", err
		}
		if threadLang == nil {
			authorLang, err = lookupAuthorContentLanguage(ctx, tx, authorPubkey, noteID, nowUnix)
			if err != nil {
				return nil, nil, "", err
			}
		}
	}
	lang, conf, source := decideNoteLanguage(tag, tagOK, contentLang, contentConf, letters, threadLang, authorLang, cfg.MinChars)
	return lang, conf, source, nil
}

func loadNoteTagsTx(ctx context.Context, tx pgx.Tx, noteID string) ([][]string, error) {
	var raw []byte
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(raw_json->'tags', '[]'::jsonb)
		FROM events
		WHERE id = $1
	`, noteID).Scan(&raw); err != nil {
		return nil, fmt.Errorf("load note tags: %w", err)
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var tags [][]string
	if err := json.Unmarshal(raw, &tags); err != nil {
		// A malformed tag list is not a language signal.
		return nil, nil
	}
	return tags, nil
}

func lookupThreadContentLanguage(ctx context.Context, tx pgx.Tx, noteID string) (*string, error) {
	var parent *string
	var root *string
	err := tx.QueryRow(ctx, `
		SELECT parent_lang.primary_language, root_lang.primary_language
		FROM thread_edges te
		LEFT JOIN note_discovery_stats parent_lang
		  ON parent_lang.event_id = te.parent_event_id
		 AND parent_lang.language_source IN ('content', 'tag')
		 AND parent_lang.primary_language IS NOT NULL
		LEFT JOIN note_discovery_stats root_lang
		  ON root_lang.event_id = te.root_event_id
		 AND te.root_event_id IS DISTINCT FROM te.parent_event_id
		 AND te.root_event_id IS DISTINCT FROM te.child_event_id
		 AND root_lang.language_source IN ('content', 'tag')
		 AND root_lang.primary_language IS NOT NULL
		WHERE te.child_event_id = $1
		  AND te.parent_missing = FALSE
	`, noteID).Scan(&parent, &root)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load thread language: %w", err)
	}
	if parent != nil && strings.TrimSpace(*parent) != "" {
		return parent, nil
	}
	if root != nil && strings.TrimSpace(*root) != "" {
		return root, nil
	}
	return nil, nil
}

func lookupAuthorContentLanguage(ctx context.Context, tx pgx.Tx, authorPubkey, noteID string, nowUnix int64) (*string, error) {
	cutoff := nowUnix - languageAuthorPriorWindow
	rows, err := tx.Query(ctx, `
		SELECT primary_language, COUNT(*)::bigint
		FROM note_discovery_stats
		WHERE author_pubkey = $1
		  AND event_id <> $2
		  AND created_at >= $3
		  AND language_source IN ('content', 'tag')
		  AND primary_language IS NOT NULL
		GROUP BY primary_language
		ORDER BY COUNT(*) DESC, primary_language ASC
		LIMIT 2
	`, authorPubkey, noteID, cutoff)
	if err != nil {
		return nil, fmt.Errorf("load author language: %w", err)
	}
	defer rows.Close()
	counts := make([]authorLanguageCount, 0, 2)
	for rows.Next() {
		var row authorLanguageCount
		if err := rows.Scan(&row.language, &row.count); err != nil {
			return nil, fmt.Errorf("scan author language: %w", err)
		}
		counts = append(counts, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author language: %w", err)
	}
	return dominantAuthorLanguage(counts), nil
}

func nullableLanguageSource(source string) any {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil
	}
	return source
}
