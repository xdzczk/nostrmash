package jobs

import (
	"context"
	"time"
)

const eventRelaysRetentionTarget = "event_relays"

// EventRelaysRetentionPurger deletes stale event_relays provenance rows,
// retaining the earliest-seen row per event. Satisfied by
// *store.PostgresStore.
type EventRelaysRetentionPurger interface {
	PurgeStaleEventRelays(ctx context.Context, seenBefore time.Time, limit int) (int64, error)
}

// EventRelaysRetentionConfig is the narrow projection of
// config.WorkerEventRelaysRetentionConfig the loop needs.
type EventRelaysRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunEventRelaysRetentionLoop periodically prunes duplicate event_relays
// provenance older than MaxAge (first-provenance per event always survives).
// Uses the shared auto-pacing drain. Blocks until ctx is done.
func RunEventRelaysRetentionLoop(ctx context.Context, log RetentionLogger, purger EventRelaysRetentionPurger, cfg EventRelaysRetentionConfig) {
	if !cfg.Enabled {
		log.Info("event_relays_retention_disabled")
		return
	}
	if cfg.MaxAge <= 0 || cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"event_relays_retention_invalid_config",
			"max_age", cfg.MaxAge.String(),
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"event_relays_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)

	runRetentionTicker(ctx, cfg.RunInterval, eventRelaysRetentionDrain(log, purger, cfg))
}

func eventRelaysRetentionDrain(log RetentionLogger, purger EventRelaysRetentionPurger, cfg EventRelaysRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: eventRelaysRetentionTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "event_relays_retention_purged",
		failedEvent:  "event_relays_retention_purge_failed",
		catchupEvent: "event_relays_retention_catchup",
		purge: func(ctx context.Context) (int64, []any, error) {
			seenBefore := time.Now().UTC().Add(-cfg.MaxAge)
			deleted, err := purger.PurgeStaleEventRelays(ctx, seenBefore, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, []any{"seen_before", seenBefore.Format(time.RFC3339)}, nil
		},
	}
}

func runEventRelaysRetentionDrain(ctx context.Context, log RetentionLogger, purger EventRelaysRetentionPurger, cfg EventRelaysRetentionConfig) {
	eventRelaysRetentionDrain(log, purger, cfg).run(ctx)
}
