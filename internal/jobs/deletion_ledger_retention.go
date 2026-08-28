package jobs

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// deletionLedgerRetentionTarget is the bounded metric label for orphan
// tombstone purge runs/rows (shared retention metric vectors).
const deletionLedgerRetentionTarget = "deletion_ledger"

// DeletionLedgerPurger purges one keyset window of orphan deletion_events
// tombstones. Satisfied by *store.PostgresStore via retention.Retention.
type DeletionLedgerPurger interface {
	PurgeOrphanDeletionLedger(ctx context.Context, cursorCreatedAt int64, cursorEventID string, createdBefore time.Time, limit int) (scanned, deleted, lastCreatedAt int64, lastEventID string, err error)
}

// DeletionLedgerRetentionConfig is the narrow projection of
// config.WorkerDeletionLedgerRetentionConfig the loop needs.
type DeletionLedgerRetentionConfig struct {
	Enabled        bool
	MaxAge         time.Duration
	RunInterval    time.Duration
	ScanBatchLimit int
}

// RunDeletionLedgerRetentionLoop periodically sweeps the deletion_events
// tombstone ledger, deleting rows older than MaxAge whose target event is not
// stored (see PurgeOrphanDeletionLedger for why those rows are dead weight).
//
// Unlike the retentionDrain loops, saturation is judged on rows *scanned*
// (not deleted): a window full of keepers must still advance the cursor
// rather than terminate the run, so the loop tracks its own composite
// (created_at, event_id) keyset cursor and walks the eligible range once per
// tick. The cursor restarts at zero each tick, which re-verifies keepers
// whose target has since been purged. Blocks until ctx is done.
func RunDeletionLedgerRetentionLoop(ctx context.Context, log RetentionLogger, purger DeletionLedgerPurger, cfg DeletionLedgerRetentionConfig) {
	if !cfg.Enabled {
		log.Info("deletion_ledger_retention_disabled")
		return
	}
	if cfg.MaxAge <= 0 || cfg.RunInterval <= 0 || cfg.ScanBatchLimit <= 0 {
		log.Error(
			"deletion_ledger_retention_invalid_config",
			"max_age", cfg.MaxAge.String(),
			"run_interval", cfg.RunInterval.String(),
			"scan_batch_limit", cfg.ScanBatchLimit,
		)
		return
	}
	log.Info(
		"deletion_ledger_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"run_interval", cfg.RunInterval.String(),
		"scan_batch_limit", cfg.ScanBatchLimit,
	)

	ticker := time.NewTicker(cfg.RunInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		runDeletionLedgerSweep(ctx, log, purger, cfg)
	}
}

// runDeletionLedgerSweep walks one full pass over tombstones older than the
// cutoff in ScanBatchLimit windows, pausing retentionCatchupPause between
// windows so interactive queries interleave.
func runDeletionLedgerSweep(ctx context.Context, log RetentionLogger, purger DeletionLedgerPurger, cfg DeletionLedgerRetentionConfig) {
	createdBefore := time.Now().UTC().Add(-cfg.MaxAge)
	var cursorCreatedAt int64
	var cursorEventID string
	var totalScanned, totalDeleted int64
	for {
		scanned, deleted, lastCreatedAt, lastEventID, err := purger.PurgeOrphanDeletionLedger(
			ctx, cursorCreatedAt, cursorEventID, createdBefore, cfg.ScanBatchLimit,
		)
		if err != nil {
			metrics.IncRetentionPurgeRun(deletionLedgerRetentionTarget, "error")
			log.Error("deletion_ledger_retention_purge_failed", "error", err)
			return
		}
		metrics.IncRetentionPurgeRun(deletionLedgerRetentionTarget, "ok")
		metrics.AddRetentionPurgedRows(deletionLedgerRetentionTarget, deleted)
		totalScanned += scanned
		totalDeleted += deleted
		if int(scanned) < cfg.ScanBatchLimit {
			break
		}
		cursorCreatedAt, cursorEventID = lastCreatedAt, lastEventID
		select {
		case <-ctx.Done():
			return
		case <-time.After(retentionCatchupPause):
		}
	}
	if totalDeleted > 0 {
		log.Info(
			"deletion_ledger_retention_purged",
			"deleted", totalDeleted,
			"scanned", totalScanned,
			"created_before", createdBefore.Format(time.RFC3339),
		)
	}
}
