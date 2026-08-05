package jobs

import (
	"context"
	"time"
)

const eventTagsRetentionTarget = "event_tags"

// EventTagsRetentionPurger deletes event_tags rows excluded by the ingest
// allowlist. Satisfied by *store.PostgresStore.
type EventTagsRetentionPurger interface {
	PruneFilteredEventTags(ctx context.Context, limit int) (int64, error)
}

// EventTagsRetentionConfig is the narrow projection of
// config.WorkerEventTagsRetentionConfig the loop needs.
type EventTagsRetentionConfig struct {
	Enabled          bool
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunEventTagsRetentionLoop periodically prunes event_tags rows that ingest
// no longer writes (junk tag names + kind-scoped contact/relay list tags).
// Uses the shared auto-pacing drain so the historical backlog empties at
// disk speed. Blocks until ctx is done.
func RunEventTagsRetentionLoop(ctx context.Context, log RetentionLogger, purger EventTagsRetentionPurger, cfg EventTagsRetentionConfig) {
	if !cfg.Enabled {
		log.Info("event_tags_retention_disabled")
		return
	}
	if cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"event_tags_retention_invalid_config",
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"event_tags_retention_enabled",
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)

	runRetentionTicker(ctx, cfg.RunInterval, eventTagsRetentionDrain(log, purger, cfg))
}

func eventTagsRetentionDrain(log RetentionLogger, purger EventTagsRetentionPurger, cfg EventTagsRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: eventTagsRetentionTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "event_tags_retention_purged",
		failedEvent:  "event_tags_retention_purge_failed",
		catchupEvent: "event_tags_retention_catchup",
		purge: func(ctx context.Context) (int64, []any, error) {
			deleted, err := purger.PruneFilteredEventTags(ctx, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, nil, nil
		},
	}
}

func runEventTagsRetentionDrain(ctx context.Context, log RetentionLogger, purger EventTagsRetentionPurger, cfg EventTagsRetentionConfig) {
	eventTagsRetentionDrain(log, purger, cfg).run(ctx)
}
