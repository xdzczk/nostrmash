package meili

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
	if kind == 1 || kind == 30023 {
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
		  AND kind IN (0, 1, 30023)
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
		case 1, 30023:
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
