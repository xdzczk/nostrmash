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
// of events per batch and three sequential Meilisearch tasks per batch
// (notes, profiles, documents — each individually waited on), the call
// normally completes in a few seconds. But each waitForTask shares
// Meilisearch's per-index FIFO task queue with any other traffic hitting
// the same indexes (notably a concurrent FullSync), so under host
// contention or a concurrent full reindex a single task can take tens of
// seconds; three of those in sequence can exceed a couple of minutes even
// though Meilisearch itself is healthy and making progress. Production
// observed batches of ~500-1000 documents repeatedly timing out at 2
// minutes and being fully re-queued (net zero drain) even though the
// underlying Meilisearch tasks went on to succeed seconds later. 8 minutes
// gives enough headroom to ride out that contention while still bounding
// a genuinely stuck/unreachable Meilisearch.
const syncEventsBatchTimeout = 8 * time.Minute

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

// fullSyncAdvisoryLockKey is an arbitrary, globally unique advisory lock
// key used to ensure at most one full sync runs across the fleet at a
// time. The api and worker services each independently check NeedsSync
// and trigger a full sync on startup; without coordination, a restart of
// both services at once (e.g. a Coolify redeploy) makes each stream the
// entire notes/profiles/documents corpus into Meilisearch concurrently,
// doubling load on an already resource-constrained instance and starving
// the incremental sweeper's batches of task-queue time.
const fullSyncAdvisoryLockKey = 872913460318

// RunStartupFullSyncIfNeeded checks whether Meilisearch's indexes are
// stale relative to Postgres and, if so, runs FullSync — but only if no
// other instance in the fleet already holds the full-sync advisory lock.
// Instances that lose the race return (stats, false, nil) and rely on the
// winner's sync (or a future NeedsSync check) to catch up.
func (c *Client) RunStartupFullSyncIfNeeded(ctx context.Context, pool *pgxpool.Pool, batchSize int) (SyncStats, bool, error) {
	if !c.Enabled() || pool == nil {
		return SyncStats{}, false, nil
	}
	needsSync, err := c.NeedsSync(ctx, pool)
	if err != nil || !needsSync {
		return SyncStats{}, false, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return SyncStats{}, false, fmt.Errorf("acquire connection for full sync lock: %w", err)
	}
	defer conn.Release()

	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, int64(fullSyncAdvisoryLockKey)).Scan(&acquired); err != nil {
		return SyncStats{}, false, fmt.Errorf("acquire meilisearch full sync advisory lock: %w", err)
	}
	if !acquired {
		return SyncStats{}, false, nil
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, int64(fullSyncAdvisoryLockKey))
	}()

	stats, err := c.FullSync(ctx, pool, batchSize)
	return stats, true, err
}

// fullSyncPacingBatchInterval bounds how many batches FullSync enqueues
// before pausing to wait for one of them to finish. Enqueuing every batch
// of a multi-hundred-thousand-row stream with zero pacing (the original
// behavior) lets Meilisearch's task queue balloon to the entire stream's
// worth of tasks almost instantly; Meilisearch's own autobatcher then
// coalesces long runs of queued tasks into single multi-hundred-thousand
// document indexing operations that pin ~1 CPU core for minutes at a time,
// starving /search reads and the incremental sweeper for the whole sync.
// Waiting periodically caps Meilisearch's queue depth and gives it — and
// search traffic — small breathing gaps between indexing operations instead
// of one unbroken flood.
const fullSyncPacingBatchInterval = 5

// fullSyncPacingDelay is a small pause taken at each pacing checkpoint
// (after fullSyncPacingBatchInterval batches, once their tasks have
// finished) purely to leave Meilisearch idle for a beat before the next
// burst, rather than immediately queuing the next interval's worth of work.
const fullSyncPacingDelay = 500 * time.Millisecond

// fullSyncPacer tracks enqueued-but-unwaited batches for one stream
// (notes/profiles/documents) and periodically waits on the most recently
// enqueued task plus a short sleep, bounding how far Meilisearch's task
// queue can get ahead of what has actually finished.
type fullSyncPacer struct {
	client      *Client
	batches     int
	lastTaskUID int64
}

func (p *fullSyncPacer) recordTask(ctx context.Context, taskUID int64) error {
	if taskUID != 0 {
		p.lastTaskUID = taskUID
	}
	p.batches++
	if !shouldPacingCheckpoint(p.batches) {
		return nil
	}
	return p.checkpoint(ctx)
}

// shouldPacingCheckpoint reports whether the batch count just reached a
// pacing interval boundary. Extracted as a pure function so the interval
// arithmetic is unit-testable without a Meilisearch client.
func shouldPacingCheckpoint(batches int) bool {
	return batches > 0 && batches%fullSyncPacingBatchInterval == 0
}

func (p *fullSyncPacer) checkpoint(ctx context.Context) error {
	if p.lastTaskUID == 0 {
		return nil
	}
	if err := p.client.waitForTask(ctx, p.lastTaskUID); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(fullSyncPacingDelay):
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
	// Capture Postgres now() (not Go wall clock) so the post-success prune
	// compares against the same clock that writes pending_meilisearch_syncs.marked_at.
	// Rows marked after this instant may cover events the keyset streams
	// skipped or documents that changed mid-sync, so they must survive.
	var syncStartedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT now()`).Scan(&syncStartedAt); err != nil {
		return SyncStats{}, fmt.Errorf("capture meilisearch full sync start time: %w", err)
	}
	stats := SyncStats{}
	notesPacer := &fullSyncPacer{client: c}
	if err := streamNotes(ctx, pool, batchSize, func(rows []NoteDocument) error {
		taskUID, err := c.enqueueNotes(ctx, rows)
		if err != nil {
			return err
		}
		stats.Notes += int64(len(rows))
		return notesPacer.recordTask(ctx, taskUID)
	}); err != nil {
		return stats, err
	}
	if err := notesPacer.checkpoint(ctx); err != nil {
		return stats, fmt.Errorf("wait notes full sync tasks: %w", err)
	}
	profilesPacer := &fullSyncPacer{client: c}
	if err := streamProfiles(ctx, pool, batchSize, func(rows []ProfileDocument) error {
		taskUID, err := c.enqueueProfiles(ctx, rows)
		if err != nil {
			return err
		}
		stats.Profiles += int64(len(rows))
		return profilesPacer.recordTask(ctx, taskUID)
	}); err != nil {
		return stats, err
	}
	if err := profilesPacer.checkpoint(ctx); err != nil {
		return stats, fmt.Errorf("wait profiles full sync tasks: %w", err)
	}
	documentsPacer := &fullSyncPacer{client: c}
	if err := streamSearchDocuments(ctx, pool, batchSize, func(rows []SearchDocument) error {
		taskUID, err := c.enqueueDocuments(ctx, rows)
		if err != nil {
			return err
		}
		stats.Documents += int64(len(rows))
		return documentsPacer.recordTask(ctx, taskUID)
	}); err != nil {
		return stats, err
	}
	if err := documentsPacer.checkpoint(ctx); err != nil {
		return stats, fmt.Errorf("wait documents full sync tasks: %w", err)
	}
	// FullSync already upserted every note/profile/document that was in
	// Postgres at syncStartedAt. Drop the redundant sweeper backlog so we
	// don't re-HTTP the same events. Partial failures above return early
	// without pruning — the pending queue remains the recovery path.
	if _, err := prunePendingMeilisearchSyncs(ctx, pool, syncStartedAt); err != nil {
		return stats, err
	}
	return stats, nil
}

// prunePendingMeilisearchSyncs deletes pending sweeper rows whose marked_at
// is at or before syncStartedAt. Extracted for unit testing without a live
// Meilisearch instance.
func prunePendingMeilisearchSyncs(ctx context.Context, pool *pgxpool.Pool, syncStartedAt time.Time) (int64, error) {
	if pool == nil {
		return 0, nil
	}
	tag, err := pool.Exec(ctx, `
		DELETE FROM pending_meilisearch_syncs
		WHERE marked_at <= $1
	`, syncStartedAt)
	if err != nil {
		return 0, fmt.Errorf("prune pending meilisearch syncs after full sync: %w", err)
	}
	return tag.RowsAffected(), nil
}
