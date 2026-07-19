package jobs

import (
	"context"
	"time"
)

// deletionRetentionTarget is the bounded metric label for processed deletion
// purge runs/rows (reuses the shared retention metric vectors in
// internal/metrics).
const deletionRetentionTarget = "deletion_events"

// DeletionRetentionPurger deletes a bounded batch of processed raw deletion
// events (kind 5). Defined as an interface so the loop can be unit tested
// without importing internal/store. Satisfied by *store.PostgresStore.
type DeletionRetentionPurger interface {
	PurgeProcessedDeletionEvents(ctx context.Context, createdBefore time.Time, deadGraceBefore time.Time, limit int) (int64, error)
}

// DeletionRetentionConfig is the narrow projection of
// config.WorkerDeletionRetentionConfig the loop needs.
type DeletionRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	DeadGrace        time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunDeletionRetentionLoop periodically purges raw deletion events (kind 5)
// older than MaxAge whose derivation has completed, skipping events whose
// derivation is still in-flight (or recently dead within DeadGrace). The
// distilled deletion_events ledger row survives. It uses the same auto-pacing
// drain as the other retention loops: a saturated batch immediately re-runs
// after retentionCatchupPause until a batch comes back below the limit, so
// DeleteBatchLimit chunks work rather than capping throughput. Blocks until ctx
// is done.
func RunDeletionRetentionLoop(ctx context.Context, log RetentionLogger, purger DeletionRetentionPurger, cfg DeletionRetentionConfig) {
	if !cfg.Enabled {
		log.Info("deletion_retention_disabled")
		return
	}
	if cfg.MaxAge <= 0 || cfg.DeadGrace <= 0 || cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"deletion_retention_invalid_config",
			"max_age", cfg.MaxAge.String(),
			"dead_grace", cfg.DeadGrace.String(),
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"deletion_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"dead_grace", cfg.DeadGrace.String(),
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)

	runRetentionTicker(ctx, cfg.RunInterval, deletionRetentionDrain(log, purger, cfg))
}

func deletionRetentionDrain(log RetentionLogger, purger DeletionRetentionPurger, cfg DeletionRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: deletionRetentionTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "deletion_retention_purged",
		failedEvent:  "deletion_retention_purge_failed",
		catchupEvent: "deletion_retention_catchup",
		purge: func(ctx context.Context) (int64, []any, error) {
			now := time.Now().UTC()
			createdBefore := now.Add(-cfg.MaxAge)
			deadGraceBefore := now.Add(-cfg.DeadGrace)
			deleted, err := purger.PurgeProcessedDeletionEvents(ctx, createdBefore, deadGraceBefore, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, []any{
				"created_before", createdBefore.Format(time.RFC3339),
				"dead_grace_before", deadGraceBefore.Format(time.RFC3339),
			}, nil
		},
	}
}

func runDeletionRetentionDrain(ctx context.Context, log RetentionLogger, purger DeletionRetentionPurger, cfg DeletionRetentionConfig) {
	deletionRetentionDrain(log, purger, cfg).run(ctx)
}
