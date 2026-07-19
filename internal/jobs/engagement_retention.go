package jobs

import (
	"context"
	"time"
)

// engagementRetentionTarget is the bounded metric label for engagement purge
// runs/rows (reuses the shared retention metric vectors in internal/metrics).
const engagementRetentionTarget = "engagement_events"

// EngagementRetentionPurger deletes a bounded batch of expired raw engagement
// events (kinds 6/7/9735). Defined as an interface so the loop can be unit
// tested and so this package does not need to import internal/store (which
// would invert the module direction). Satisfied by *store.PostgresStore.
type EngagementRetentionPurger interface {
	PurgeExpiredEngagementEvents(ctx context.Context, createdBefore time.Time, deadGraceBefore time.Time, limit int) (int64, error)
}

// EngagementRetentionConfig is the narrow projection of
// config.WorkerEngagementRetentionConfig the loop needs.
type EngagementRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	DeadGrace        time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunEngagementRetentionLoop periodically purges raw engagement events older
// than MaxAge, skipping events whose derivation is still in-flight (or recently
// dead within DeadGrace). It uses the same auto-pacing drain as the job
// retention loop: a saturated batch immediately re-runs after
// retentionCatchupPause until a batch comes back below the limit, so
// DeleteBatchLimit chunks work rather than capping throughput. Blocks until ctx
// is done.
func RunEngagementRetentionLoop(ctx context.Context, log RetentionLogger, purger EngagementRetentionPurger, cfg EngagementRetentionConfig) {
	if !cfg.Enabled {
		log.Info("engagement_retention_disabled")
		return
	}
	if cfg.MaxAge <= 0 || cfg.DeadGrace <= 0 || cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"engagement_retention_invalid_config",
			"max_age", cfg.MaxAge.String(),
			"dead_grace", cfg.DeadGrace.String(),
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"engagement_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"dead_grace", cfg.DeadGrace.String(),
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)

	runRetentionTicker(ctx, cfg.RunInterval, engagementRetentionDrain(log, purger, cfg))
}

func engagementRetentionDrain(log RetentionLogger, purger EngagementRetentionPurger, cfg EngagementRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: engagementRetentionTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "engagement_retention_purged",
		failedEvent:  "engagement_retention_purge_failed",
		catchupEvent: "engagement_retention_catchup",
		purge: func(ctx context.Context) (int64, []any, error) {
			now := time.Now().UTC()
			createdBefore := now.Add(-cfg.MaxAge)
			deadGraceBefore := now.Add(-cfg.DeadGrace)
			deleted, err := purger.PurgeExpiredEngagementEvents(ctx, createdBefore, deadGraceBefore, cfg.DeleteBatchLimit)
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

func runEngagementRetentionDrain(ctx context.Context, log RetentionLogger, purger EngagementRetentionPurger, cfg EngagementRetentionConfig) {
	engagementRetentionDrain(log, purger, cfg).run(ctx)
}
