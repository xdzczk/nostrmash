package meili

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FullSync streams can legitimately exceed the production 15s statement_timeout
// guardrail (first notes page alone is multi-second even with kind/created
// indexes once content + lateral title joins are included). Elevate only the
// acquired sync connection; API request paths keep the short default.
const syncStatementTimeout = 5 * time.Minute

func withSyncDB(ctx context.Context, pool *pgxpool.Pool, fn func(conn *pgxpool.Conn) error) error {
	if pool == nil {
		return fmt.Errorf("nil pool")
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire sync db conn: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		"SELECT set_config('statement_timeout', '%d', false)",
		syncStatementTimeout.Milliseconds(),
	)); err != nil {
		return fmt.Errorf("raise sync statement_timeout: %w", err)
	}
	defer func() {
		// Return the pooled connection to the DB/role default (15s in prod).
		_, _ = conn.Exec(context.Background(), `RESET statement_timeout`)
	}()
	return fn(conn)
}

func streamNotes(ctx context.Context, pool *pgxpool.Pool, batchSize int, consume func([]NoteDocument) error) error {
	return withSyncDB(ctx, pool, func(conn *pgxpool.Conn) error {
		return streamNotesConn(ctx, conn, batchSize, consume)
	})
}

func streamNotesConn(ctx context.Context, conn *pgxpool.Conn, batchSize int, consume func([]NoteDocument) error) error {
	// Keyset on (created_at DESC, id DESC). OFFSET pagination was timing out
	// FullSync in production: deep pages were multi-minute sequential scans.
	minCreatedAt := indexedNotesMinCreatedAt(time.Now())
	var (
		haveCursor bool
		cursorAt   int64
		cursorID   string
	)
	for {
		var (
			rows pgx.Rows
			err  error
		)
		if !haveCursor {
			rows, err = conn.Query(ctx, noteDocumentSelect+`
				WHERE e.kind IN (1, 30023)
				  AND e.created_at >= $2::bigint
				ORDER BY e.created_at DESC, e.id DESC
				LIMIT $1
			`, batchSize, minCreatedAt)
		} else {
			rows, err = conn.Query(ctx, noteDocumentSelect+`
				WHERE e.kind IN (1, 30023)
				  AND e.created_at >= $4::bigint
				  AND (e.created_at, e.id) < ($2::bigint, $3::text)
				ORDER BY e.created_at DESC, e.id DESC
				LIMIT $1
			`, batchSize, cursorAt, cursorID, minCreatedAt)
		}
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
		last := batch[len(batch)-1]
		haveCursor = true
		cursorAt = last.CreatedAt
		cursorID = last.ID
	}
}

func streamProfiles(ctx context.Context, pool *pgxpool.Pool, batchSize int, consume func([]ProfileDocument) error) error {
	return withSyncDB(ctx, pool, func(conn *pgxpool.Conn) error {
		return streamProfilesConn(ctx, conn, batchSize, consume)
	})
}

func streamProfilesConn(ctx context.Context, conn *pgxpool.Conn, batchSize int, consume func([]ProfileDocument) error) error {
	// Keyset on (metadata_created_at DESC, pubkey ASC). Mixed sort directions
	// mean the continue predicate is not a single row comparison.
	var (
		haveCursor bool
		cursorAt   int64
		cursorPK   string
	)
	const profileSelect = `
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
	`
	for {
		var (
			rows pgx.Rows
			err  error
		)
		if !haveCursor {
			rows, err = conn.Query(ctx, profileSelect+`
				ORDER BY p.metadata_created_at DESC, p.pubkey ASC
				LIMIT $1
			`, batchSize)
		} else {
			rows, err = conn.Query(ctx, profileSelect+`
				WHERE p.metadata_created_at < $2::bigint
				   OR (p.metadata_created_at = $2::bigint AND p.pubkey > $3::text)
				ORDER BY p.metadata_created_at DESC, p.pubkey ASC
				LIMIT $1
			`, batchSize, cursorAt, cursorPK)
		}
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
		last := batch[len(batch)-1]
		haveCursor = true
		cursorAt = last.MetadataCreatedAt
		cursorPK = last.Pubkey
	}
}

func streamSearchDocuments(ctx context.Context, pool *pgxpool.Pool, batchSize int, consume func([]SearchDocument) error) error {
	return withSyncDB(ctx, pool, func(conn *pgxpool.Conn) error {
		return streamSearchDocumentsConn(ctx, conn, batchSize, consume)
	})
}

func streamSearchDocumentsConn(ctx context.Context, conn *pgxpool.Conn, batchSize int, consume func([]SearchDocument) error) error {
	// Keyset on (entity_type ASC, entity_id ASC).
	var (
		haveCursor   bool
		cursorType   string
		cursorEntity string
	)
	for {
		var (
			rows pgx.Rows
			err  error
		)
		if !haveCursor {
			rows, err = conn.Query(ctx, `
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
				WHERE entity_type IN ('hashtag', 'identity', 'relay')
				ORDER BY entity_type ASC, entity_id ASC
				LIMIT $1
			`, batchSize)
		} else {
			rows, err = conn.Query(ctx, `
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
				WHERE entity_type IN ('hashtag', 'identity', 'relay')
				  AND (entity_type, entity_id) > ($2::text, $3::text)
				ORDER BY entity_type ASC, entity_id ASC
				LIMIT $1
			`, batchSize, cursorType, cursorEntity)
		}
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
		last := batch[len(batch)-1]
		haveCursor = true
		cursorType = last.EntityType
		cursorEntity = last.EntityID
	}
}
