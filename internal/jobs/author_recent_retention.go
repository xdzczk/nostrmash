package jobs

import (
	"context"
	"time"
)

const authorRecentRetentionTarget = "author_recent_events"

// AuthorRecentRetentionPruner bounds the author_recent_events projection.
// Satisfied by *store.PostgresStore.
type AuthorRecentRetentionPruner interface {
	PruneAuthorRecentEvents(ctx context.Context, olderThan time.Time, perAuthorCap, authorBatchLimit, deleteBatchLimit int) (int64, error)
}

// AuthorRecentRetentionConfig is the narrow projection of
// config.WorkerAuthorRecentRetentionConfig the loop needs.
type AuthorRecentRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	PerAuthorCap     int
	AuthorBatchLimit int
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunAuthorRecentRetentionLoop periodically prunes author_recent_events rows
// past the age horizon or beyond the per-author cap. Uses the shared
// auto-pacing drain. Blocks until ctx is done.
func RunAuthorRecentRetentionLoop(ctx context.Context, log RetentionLogger, pruner AuthorRecentRetentionPruner, cfg AuthorRecentRetentionConfig) {
	if !cfg.Enabled {
		log.Info("author_recent_retention_disabled")
		return
	}
	if cfg.MaxAge <= 0 || cfg.PerAuthorCap <= 0 || cfg.AuthorBatchLimit <= 0 || cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"author_recent_retention_invalid_config",
			"max_age", cfg.MaxAge.String(),
			"per_author_cap", cfg.PerAuthorCap,
			"author_batch_limit", cfg.AuthorBatchLimit,
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"author_recent_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"per_author_cap", cfg.PerAuthorCap,
		"author_batch_limit", cfg.AuthorBatchLimit,
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)

	runRetentionTicker(ctx, cfg.RunInterval, authorRecentRetentionDrain(log, pruner, cfg))
}

func authorRecentRetentionDrain(log RetentionLogger, pruner AuthorRecentRetentionPruner, cfg AuthorRecentRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: authorRecentRetentionTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "author_recent_retention_pruned",
		failedEvent:  "author_recent_retention_prune_failed",
		catchupEvent: "author_recent_retention_catchup",
		purge: func(ctx context.Context) (int64, []any, error) {
			olderThan := time.Now().UTC().Add(-cfg.MaxAge)
			deleted, err := pruner.PruneAuthorRecentEvents(ctx, olderThan, cfg.PerAuthorCap, cfg.AuthorBatchLimit, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, []any{"older_than", olderThan.Format(time.RFC3339)}, nil
		},
	}
}

func runAuthorRecentRetentionDrain(ctx context.Context, log RetentionLogger, pruner AuthorRecentRetentionPruner, cfg AuthorRecentRetentionConfig) {
	authorRecentRetentionDrain(log, pruner, cfg).run(ctx)
}
