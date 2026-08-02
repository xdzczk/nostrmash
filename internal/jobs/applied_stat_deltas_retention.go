package jobs

import (
	"context"
	"time"
)

const appliedStatDeltasRetentionTarget = "applied_stat_deltas"

// AppliedStatDeltasRetentionPurger deletes orphaned applied_stat_deltas
// ledger rows (see docs/design/incremental-author-stats.md). Satisfied by
// *store.PostgresStore.
type AppliedStatDeltasRetentionPurger interface {
	PruneOrphanedAppliedStatDeltas(ctx context.Context, appliedBefore time.Time, limit int) (int64, error)
}

// AppliedStatDeltasRetentionConfig is the narrow projection of
// config.WorkerAppliedStatDeltasRetentionConfig the loop needs.
type AppliedStatDeltasRetentionConfig struct {
	Enabled          bool
	GracePeriod      time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunAppliedStatDeltasRetentionLoop periodically prunes applied_stat_deltas
// ledger rows whose source event has already been deleted (see the doc
// comment on the underlying store method for why this is the only safe
// pruning condition — a live event's ledger rows must never be pruned early,
// since a future retention purge may still need them to gate a decrement).
// Uses the shared auto-pacing drain. Blocks until ctx is done.
func RunAppliedStatDeltasRetentionLoop(ctx context.Context, log RetentionLogger, purger AppliedStatDeltasRetentionPurger, cfg AppliedStatDeltasRetentionConfig) {
	if !cfg.Enabled {
		log.Info("applied_stat_deltas_retention_disabled")
		return
	}
	if cfg.GracePeriod <= 0 || cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"applied_stat_deltas_retention_invalid_config",
			"grace_period", cfg.GracePeriod.String(),
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"applied_stat_deltas_retention_enabled",
		"grace_period", cfg.GracePeriod.String(),
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)

	runRetentionTicker(ctx, cfg.RunInterval, appliedStatDeltasRetentionDrain(log, purger, cfg))
}

func appliedStatDeltasRetentionDrain(log RetentionLogger, purger AppliedStatDeltasRetentionPurger, cfg AppliedStatDeltasRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: appliedStatDeltasRetentionTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "applied_stat_deltas_retention_purged",
		failedEvent:  "applied_stat_deltas_retention_purge_failed",
		catchupEvent: "applied_stat_deltas_retention_catchup",
		purge: func(ctx context.Context) (int64, []any, error) {
			appliedBefore := time.Now().UTC().Add(-cfg.GracePeriod)
			deleted, err := purger.PruneOrphanedAppliedStatDeltas(ctx, appliedBefore, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, []any{"applied_before", appliedBefore.Format(time.RFC3339)}, nil
		},
	}
}

func runAppliedStatDeltasRetentionDrain(ctx context.Context, log RetentionLogger, purger AppliedStatDeltasRetentionPurger, cfg AppliedStatDeltasRetentionConfig) {
	appliedStatDeltasRetentionDrain(log, purger, cfg).run(ctx)
}
