package meili

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SyncStats struct {
	Notes     int64 `json:"notes"`
	Profiles  int64 `json:"profiles"`
	Documents int64 `json:"documents"`
}

// syncEventTimeout bounds a single per-event Meilisearch sync call from the
// derivation bundle. Without this bound, waitForTask polls indefinitely if
// Meilisearch is unhealthy or its task queue is backed up, which in
// production permanently stalled every live-pool worker as soon as it
// processed its first kind=0 or kind=1 event (worker goroutines were
// observed stuck inside WaitForTaskWithContext for 12+ minutes with open
// PG transactions in "idle in transaction" state).
//
// 30s is generous enough to cover normal Meilisearch ingestion latency
// (typically a few hundred ms per task) while ensuring a degraded Meili
// can never block the derivation pipeline. The bundle treats sync errors
// as best-effort, so a timeout simply logs and moves on — the next event
// will retry, or a periodic full-sync can reconcile.
const syncEventTimeout = 30 * time.Second

func (c *Client) SyncEvent(ctx context.Context, pool *pgxpool.Pool, eventID string) error {
	if !c.Enabled() || pool == nil {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	// Bound the entire per-event sync (DB lookup + Meili upsert + task wait)
	// so a stuck Meilisearch can never wedge a worker goroutine forever.
	ctx, cancel := context.WithTimeout(ctx, syncEventTimeout)
	defer cancel()
	var (
		kind   int
		pubkey string
	)
	if err := pool.QueryRow(ctx, `
		SELECT kind, pubkey
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind, &pubkey); err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return fmt.Errorf("load event for meilisearch sync: %w", err)
	}
	if kind == 1 {
		noteDocs, err := loadNoteDocumentsByIDs(ctx, pool, []string{eventID})
		if err != nil {
			return err
		}
		if err := c.UpsertNotes(ctx, noteDocs); err != nil {
			return err
		}
		docRows, err := loadSearchDocumentsForNote(ctx, pool, eventID)
		if err != nil {
			return err
		}
		return c.UpsertDocuments(ctx, docRows)
	}
	if kind == 0 {
		profileDocs, err := loadProfileDocumentsByPubkeys(ctx, pool, []string{pubkey})
		if err != nil {
			return err
		}
		if err := c.UpsertProfiles(ctx, profileDocs); err != nil {
			return err
		}
		docRows, err := loadSearchDocumentsForProfile(ctx, pool, pubkey)
		if err != nil {
			return err
		}
		return c.UpsertDocuments(ctx, docRows)
	}
	return nil
}

// syncEventsBatchTimeout bounds the entire batched sync. With ~hundreds
// of events per batch and three Meilisearch tasks per batch (one per
// index), the call should complete in a few seconds even on slow
// Meilisearch instances. The timeout protects against pathological
// stalls without unnecessarily aborting a healthy batch.
const syncEventsBatchTimeout = 2 * time.Minute

// SyncEventsBatch is the bulk equivalent of SyncEvent: it loads notes,
// profiles and search_documents for an arbitrary list of event IDs and
// dispatches at most three Meilisearch tasks (one per index) for the
// entire batch. This collapses N×2 individual Meili tasks (which
// Meilisearch processes serially per index) into 3 tasks, removing the
// most expensive bottleneck the per-event sweeper had under heavy
// ingest.
//
// Returns an error if any of the bulk loads or Meilisearch upserts
// fail. Callers should fall back to per-event SyncEvent on failure so a
// single malformed document does not poison an otherwise healthy
// batch.
func (c *Client) SyncEventsBatch(ctx context.Context, pool *pgxpool.Pool, eventIDs []string) error {
	if !c.Enabled() || pool == nil || len(eventIDs) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, syncEventsBatchTimeout)
	defer cancel()

	rows, err := pool.Query(ctx, `
		SELECT id, kind, pubkey
		FROM events
		WHERE id = ANY($1::text[])
		  AND kind IN (0, 1)
	`, eventIDs)
	if err != nil {
		return fmt.Errorf("load batch event metadata for meilisearch sync: %w", err)
	}
	noteIDs := make([]string, 0, len(eventIDs))
	profilePubkeys := make([]string, 0, len(eventIDs))
	seenPubkey := make(map[string]struct{}, len(eventIDs))
	for rows.Next() {
		var (
			id     string
			kind   int
			pubkey string
		)
		if err := rows.Scan(&id, &kind, &pubkey); err != nil {
			rows.Close()
			return fmt.Errorf("scan batch event row: %w", err)
		}
		switch kind {
		case 1:
			noteIDs = append(noteIDs, id)
		case 0:
			if _, ok := seenPubkey[pubkey]; !ok {
				seenPubkey[pubkey] = struct{}{}
				profilePubkeys = append(profilePubkeys, pubkey)
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read batch event rows: %w", err)
	}
	rows.Close()

	if len(noteIDs) > 0 {
		noteDocs, err := loadNoteDocumentsByIDs(ctx, pool, noteIDs)
		if err != nil {
			return err
		}
		if err := c.UpsertNotes(ctx, noteDocs); err != nil {
			return err
		}
	}
	if len(profilePubkeys) > 0 {
		profileDocs, err := loadProfileDocumentsByPubkeys(ctx, pool, profilePubkeys)
		if err != nil {
			return err
		}
		if err := c.UpsertProfiles(ctx, profileDocs); err != nil {
			return err
		}
	}

	searchDocs, err := loadSearchDocumentsForBatch(ctx, pool, noteIDs, profilePubkeys)
	if err != nil {
		return err
	}
	if len(searchDocs) > 0 {
		if err := c.UpsertDocuments(ctx, searchDocs); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) FullSync(ctx context.Context, pool *pgxpool.Pool, batchSize int) (SyncStats, error) {
	if !c.Enabled() || pool == nil {
		return SyncStats{}, nil
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	if err := c.EnsureIndexes(ctx); err != nil {
		return SyncStats{}, err
	}
	stats := SyncStats{}
	if err := streamNotes(ctx, pool, batchSize, func(rows []NoteDocument) error {
		if err := c.UpsertNotes(ctx, rows); err != nil {
			return err
		}
		stats.Notes += int64(len(rows))
		return nil
	}); err != nil {
		return stats, err
	}
	if err := streamProfiles(ctx, pool, batchSize, func(rows []ProfileDocument) error {
		if err := c.UpsertProfiles(ctx, rows); err != nil {
			return err
		}
		stats.Profiles += int64(len(rows))
		return nil
	}); err != nil {
		return stats, err
	}
	if err := streamSearchDocuments(ctx, pool, batchSize, func(rows []SearchDocument) error {
		if err := c.UpsertDocuments(ctx, rows); err != nil {
			return err
		}
		stats.Documents += int64(len(rows))
		return nil
	}); err != nil {
		return stats, err
	}
	return stats, nil
}

func streamNotes(ctx context.Context, pool *pgxpool.Pool, batchSize int, consume func([]NoteDocument) error) error {
	offset := 0
	for {
		rows, err := pool.Query(ctx, `
			SELECT
				e.id,
				coalesce(e.content, ''),
				e.pubkey,
				e.created_at,
				coalesce(nds.primary_language, 'und')
			FROM events e
			LEFT JOIN note_discovery_stats nds ON nds.event_id = e.id
			WHERE e.kind = 1
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
			row.ID = row.EntityType + "_" + row.EntityID
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

func loadNoteDocumentsByIDs(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]NoteDocument, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			e.id,
			coalesce(e.content, ''),
			e.pubkey,
			e.created_at,
			coalesce(nds.primary_language, 'und')
		FROM events e
		LEFT JOIN note_discovery_stats nds ON nds.event_id = e.id
		WHERE e.kind = 1
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
		row.ID = row.EntityType + "_" + row.EntityID
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
