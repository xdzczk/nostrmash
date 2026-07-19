package meili

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func streamNotes(ctx context.Context, pool *pgxpool.Pool, batchSize int, consume func([]NoteDocument) error) error {
	offset := 0
	for {
		rows, err := pool.Query(ctx, noteDocumentSelect+`
			WHERE e.kind IN (1, 30023)
			ORDER BY e.created_at DESC, e.id DESC
			LIMIT $1
			OFFSET $2
		`, batchSize, offset)
		if err != nil {
			return fmt.Errorf("query notes for meilisearch sync: %w", err)
		}
		batch := make([]NoteDocument, 0, batchSize)
		for rows.Next() {
			var row NoteDocument
			if err := rows.Scan(&row.ID, &row.Content, &row.Pubkey, &row.CreatedAt, &row.Language); err != nil {
				rows.Close()
				return fmt.Errorf("scan note sync row: %w", err)
			}
			batch = append(batch, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("read note sync rows: %w", err)
		}
		rows.Close()
		if len(batch) == 0 {
			return nil
		}
		if err := consume(batch); err != nil {
			return err
		}
		offset += len(batch)
	}
}

func streamProfiles(ctx context.Context, pool *pgxpool.Pool, batchSize int, consume func([]ProfileDocument) error) error {
	offset := 0
	for {
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
			ORDER BY p.metadata_created_at DESC, p.pubkey ASC
			LIMIT $1
			OFFSET $2
		`, batchSize, offset)
		if err != nil {
			return fmt.Errorf("query profiles for meilisearch sync: %w", err)
		}
		batch := make([]ProfileDocument, 0, batchSize)
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
				rows.Close()
				return fmt.Errorf("scan profile sync row: %w", err)
			}
			row.ProfileJSON = json.RawMessage(profileText)
			row.Popularity = float64(popularity)
			batch = append(batch, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("read profile sync rows: %w", err)
		}
		rows.Close()
		if len(batch) == 0 {
			return nil
		}
		if err := consume(batch); err != nil {
			return err
		}
		offset += len(batch)
	}
}

func streamSearchDocuments(ctx context.Context, pool *pgxpool.Pool, batchSize int, consume func([]SearchDocument) error) error {
	offset := 0
	for {
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
			ORDER BY entity_type ASC, entity_id ASC
			LIMIT $1
			OFFSET $2
		`, batchSize, offset)
		if err != nil {
			return fmt.Errorf("query search_documents for meilisearch sync: %w", err)
		}
		batch := make([]SearchDocument, 0, batchSize)
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
				rows.Close()
				return fmt.Errorf("scan search_documents sync row: %w", err)
			}
			row.ID = safeMeiliDocID(row.EntityType, row.EntityID)
			row.Freshness = freshness.UTC().Unix()
			batch = append(batch, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("read search_documents sync rows: %w", err)
		}
		rows.Close()
		if len(batch) == 0 {
			return nil
		}
		if err := consume(batch); err != nil {
			return err
		}
		offset += len(batch)
	}
}
