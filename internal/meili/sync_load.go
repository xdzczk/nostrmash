package meili

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func loadNoteDocumentsByIDs(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]NoteDocument, error) {
	rows, err := pool.Query(ctx, noteDocumentSelect+`
		WHERE e.kind IN (1, 30023)
		  AND e.id = ANY($1::text[])
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("query note meilisearch docs: %w", err)
	}
	defer rows.Close()
	out := make([]NoteDocument, 0, len(ids))
	for rows.Next() {
		var row NoteDocument
		if err := rows.Scan(&row.ID, &row.Content, &row.Pubkey, &row.CreatedAt, &row.Language); err != nil {
			return nil, fmt.Errorf("scan note meilisearch doc: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read note meilisearch docs: %w", err)
	}
	return out, nil
}

func loadProfileDocumentsByPubkeys(ctx context.Context, pool *pgxpool.Pool, pubkeys []string) ([]ProfileDocument, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			p.pubkey,
			p.metadata_event_id,
			p.metadata_created_at,
			coalesce(p.name, ''),
			coalesce(p.display_name, ''),
			coalesce(p.about, ''),
			coalesce(p.nip05, ''),
			p.profile_json::text,
			coalesce(stats.follower_count, 0) + coalesce(stats.note_count, 0)
		FROM profiles_latest p
		LEFT JOIN profile_public_stats stats ON stats.pubkey = p.pubkey
		WHERE p.pubkey = ANY($1::text[])
	`, pubkeys)
	if err != nil {
		return nil, fmt.Errorf("query profile meilisearch docs: %w", err)
	}
	defer rows.Close()
	out := make([]ProfileDocument, 0, len(pubkeys))
	for rows.Next() {
		var (
			row         ProfileDocument
			profileText string
			popularity  int64
		)
		if err := rows.Scan(
			&row.Pubkey,
			&row.MetadataEventID,
			&row.MetadataCreatedAt,
			&row.Name,
			&row.DisplayName,
			&row.About,
			&row.NIP05,
			&profileText,
			&popularity,
		); err != nil {
			return nil, fmt.Errorf("scan profile meilisearch doc: %w", err)
		}
		row.ProfileJSON = json.RawMessage(profileText)
		row.Popularity = float64(popularity)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read profile meilisearch docs: %w", err)
	}
	return out, nil
}

func loadSearchDocumentsForNote(ctx context.Context, pool *pgxpool.Pool, eventID string) ([]SearchDocument, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			entity_type,
			entity_id,
			coalesce(title, ''),
			coalesce(body, ''),
			aliases,
			identity_tokens,
			freshness,
			popularity,
			trust_score
		FROM search_documents
		WHERE (entity_type = 'note' AND entity_id = $1)
		   OR (entity_type = 'hashtag' AND entity_id IN (
		        SELECT hashtag FROM event_hashtags WHERE event_id = $1
		   ))
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("query search_documents for note sync: %w", err)
	}
	defer rows.Close()
	return scanSearchDocuments(rows)
}

// loadSearchDocumentsForBatch returns all search_documents touched by a
// batch of note event IDs and profile pubkeys in a single round-trip.
// Rows are deduplicated on (entity_type, entity_id) so the same hashtag
// referenced by multiple notes only appears once in the resulting
// upsert payload.
func loadSearchDocumentsForBatch(
	ctx context.Context,
	pool *pgxpool.Pool,
	noteIDs []string,
	profilePubkeys []string,
) ([]SearchDocument, error) {
	if len(noteIDs) == 0 && len(profilePubkeys) == 0 {
		return nil, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT
			entity_type,
			entity_id,
			coalesce(title, ''),
			coalesce(body, ''),
			aliases,
			identity_tokens,
			freshness,
			popularity,
			trust_score
		FROM search_documents
		WHERE (entity_type = 'note'    AND entity_id = ANY($1::text[]))
		   OR (entity_type = 'profile' AND entity_id = ANY($2::text[]))
		   OR (entity_type = 'hashtag' AND entity_id IN (
		         SELECT DISTINCT hashtag FROM event_hashtags
		         WHERE event_id = ANY($1::text[])
		   ))
		   OR (entity_type = 'identity' AND identity_tokens && $2::text[])
	`, noteIDs, profilePubkeys)
	if err != nil {
		return nil, fmt.Errorf("query search_documents for batch sync: %w", err)
	}
	defer rows.Close()
	return scanSearchDocuments(rows)
}

func loadSearchDocumentsForProfile(ctx context.Context, pool *pgxpool.Pool, pubkey string) ([]SearchDocument, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			entity_type,
			entity_id,
			coalesce(title, ''),
			coalesce(body, ''),
			aliases,
			identity_tokens,
			freshness,
			popularity,
			trust_score
		FROM search_documents
		WHERE (entity_type = 'profile' AND entity_id = $1)
		   OR (entity_type = 'identity' AND $1 = ANY(identity_tokens))
	`, pubkey)
	if err != nil {
		return nil, fmt.Errorf("query search_documents for profile sync: %w", err)
	}
	defer rows.Close()
	return scanSearchDocuments(rows)
}

func scanSearchDocuments(rows pgx.Rows) ([]SearchDocument, error) {
	out := make([]SearchDocument, 0)
	for rows.Next() {
		var (
			row       SearchDocument
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
		); err != nil {
			return nil, fmt.Errorf("scan search_document row: %w", err)
		}
		row.ID = safeMeiliDocID(row.EntityType, row.EntityID)
		row.Freshness = freshness.UTC().Unix()
		if row.Aliases == nil {
			row.Aliases = []string{}
		}
		if row.IdentityTokens == nil {
			row.IdentityTokens = []string{}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read search_document rows: %w", err)
	}
	return out, nil
}
