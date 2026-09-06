package jobs

import (
	"context"
	"time"
)

const followerGainEventsRetentionTarget = "follower_gain_events"

// FollowerGainEventsRetentionPurger deletes follower_gain_events rows older
// than the retention horizon (see
// migrations/000086_follower_gain_events.sql). Satisfied by
// *store.PostgresStore.
type FollowerGainEventsRetentionPurger interface {
	PruneExpiredFollowerGainEvents(ctx context.Context, createdBefore time.Time, limit int) (int64, error)
}

// FollowerGainEventsRetentionConfig is the narrow projection of
// config.WorkerFollowerGainEventsRetentionConfig the loop needs.
type FollowerGainEventsRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunFollowerGainEventsRetentionLoop periodically prunes follower_gain_events
// rows whose insert time has aged past MaxAge. Every reader windows gains to
// at most 7 days, so MaxAge only needs to exceed that; the loop is pure
// hygiene keeping the table bounded by roughly one horizon of true follower
// gains. Uses the shared auto-pacing drain. Blocks until ctx is done.
func RunFollowerGainEventsRetentionLoop(ctx context.Context, log RetentionLogger, purger FollowerGainEventsRetentionPurger, cfg FollowerGainEventsRetentionConfig) {
	if !cfg.Enabled {
		log.Info("follower_gain_events_retention_disabled")
		return
	}
	if cfg.MaxAge <= 0 || cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"follower_gain_events_retention_invalid_config",
			"max_age", cfg.MaxAge.String(),
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"follower_gain_events_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)

	runRetentionTicker(ctx, cfg.RunInterval, followerGainEventsRetentionDrain(log, purger, cfg))
}

func followerGainEventsRetentionDrain(log RetentionLogger, purger FollowerGainEventsRetentionPurger, cfg FollowerGainEventsRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: followerGainEventsRetentionTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "follower_gain_events_retention_purged",
		failedEvent:  "follower_gain_events_retention_purge_failed",
		catchupEvent: "follower_gain_events_retention_catchup",
		purge: func(ctx context.Context) (int64, []any, error) {
			createdBefore := time.Now().UTC().Add(-cfg.MaxAge)
			deleted, err := purger.PruneExpiredFollowerGainEvents(ctx, createdBefore, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, []any{"created_before", createdBefore.Format(time.RFC3339)}, nil
		},
	}
}
