package jobs

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
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

	ticker := time.NewTicker(cfg.RunInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		runAuthorRecentRetentionDrain(ctx, log, pruner, cfg)
	}
}

func runAuthorRecentRetentionDrain(ctx context.Context, log RetentionLogger, pruner AuthorRecentRetentionPruner, cfg AuthorRecentRetentionConfig) {
	consecutiveSaturated := 0
	for {
		olderThan := time.Now().UTC().Add(-cfg.MaxAge)
		deleted, err := pruner.PruneAuthorRecentEvents(ctx, olderThan, cfg.PerAuthorCap, cfg.AuthorBatchLimit, cfg.DeleteBatchLimit)
		if err != nil {
			metrics.IncRetentionPurgeRun(authorRecentRetentionTarget, "error")
			log.Error("author_recent_retention_prune_failed", "error", err)
			return
		}
		metrics.IncRetentionPurgeRun(authorRecentRetentionTarget, "ok")
		metrics.AddRetentionPurgedRows(authorRecentRetentionTarget, deleted)
		if deleted > 0 {
			log.Info(
				"author_recent_retention_pruned",
				"deleted", deleted,
				"older_than", olderThan.Format(time.RFC3339),
			)
		}
		// Both passes are bounded by DeleteBatchLimit; a combined result below
		// one batch limit means neither pass saturated.
		if int(deleted) < cfg.DeleteBatchLimit {
			return
		}
		consecutiveSaturated++
		if consecutiveSaturated%retentionCatchupReportEvery == 0 {
			log.Info(
				"author_recent_retention_catchup",
				"consecutive_full_batches", consecutiveSaturated,
				"delete_batch_limit", cfg.DeleteBatchLimit,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retentionCatchupPause):
		}
	}
}
