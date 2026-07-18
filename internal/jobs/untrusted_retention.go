package jobs

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// untrustedRetentionTarget is the bounded metric label for untrusted-author
// purge runs/rows (shared retention metric vectors in internal/metrics).
const untrustedRetentionTarget = "untrusted_author_events"

// UntrustedAuthorRetentionPurger deletes a bounded batch of author-gated raw
// events whose author is outside trust_graph_snapshot. Satisfied by
// *store.PostgresStore.
type UntrustedAuthorRetentionPurger interface {
	PurgeUntrustedAuthorEvents(ctx context.Context, olderThan time.Time, deadGraceBefore time.Time, limit int) (int64, error)
}

// UntrustedAuthorRetentionConfig is the narrow projection of
// config.WorkerUntrustedAuthorRetentionConfig the loop needs.
type UntrustedAuthorRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	DeadGrace        time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunUntrustedAuthorRetentionLoop periodically purges author-gated raw events
// (kinds 1/4/9802/10000/10003/30023) from authors outside the trust graph,
// once older than MaxAge. The store-side purge is fail-safe when the trust
// graph snapshot has never loaded (it deletes nothing), so running this loop
// on a fresh deployment is harmless. Uses the shared auto-pacing drain: a
// saturated batch immediately re-runs after retentionCatchupPause. Blocks
// until ctx is done.
func RunUntrustedAuthorRetentionLoop(ctx context.Context, log RetentionLogger, purger UntrustedAuthorRetentionPurger, cfg UntrustedAuthorRetentionConfig) {
	if !cfg.Enabled {
		log.Info("untrusted_retention_disabled")
		return
	}
	if cfg.MaxAge <= 0 || cfg.DeadGrace <= 0 || cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"untrusted_retention_invalid_config",
			"max_age", cfg.MaxAge.String(),
			"dead_grace", cfg.DeadGrace.String(),
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"untrusted_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"dead_grace", cfg.DeadGrace.String(),
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
		runUntrustedAuthorRetentionDrain(ctx, log, purger, cfg)
	}
}

func runUntrustedAuthorRetentionDrain(ctx context.Context, log RetentionLogger, purger UntrustedAuthorRetentionPurger, cfg UntrustedAuthorRetentionConfig) {
	consecutiveSaturated := 0
	for {
		now := time.Now().UTC()
		olderThan := now.Add(-cfg.MaxAge)
		deadGraceBefore := now.Add(-cfg.DeadGrace)
		deleted, err := purger.PurgeUntrustedAuthorEvents(ctx, olderThan, deadGraceBefore, cfg.DeleteBatchLimit)
		if err != nil {
			metrics.IncRetentionPurgeRun(untrustedRetentionTarget, "error")
			log.Error("untrusted_retention_purge_failed", "error", err)
			return
		}
		metrics.IncRetentionPurgeRun(untrustedRetentionTarget, "ok")
		metrics.AddRetentionPurgedRows(untrustedRetentionTarget, deleted)
		if deleted > 0 {
			log.Info(
				"untrusted_retention_purged",
				"deleted", deleted,
				"older_than", olderThan.Format(time.RFC3339),
				"dead_grace_before", deadGraceBefore.Format(time.RFC3339),
			)
		}
		if int(deleted) < cfg.DeleteBatchLimit {
			return
		}
		consecutiveSaturated++
		if consecutiveSaturated%retentionCatchupReportEvery == 0 {
			log.Info(
				"untrusted_retention_catchup",
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
