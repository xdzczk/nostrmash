package runtime

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

// RunJobRetentionLoop is a thin wrapper that delegates to
// jobs.RunRetentionLoop. Kept here so existing callers/imports do not break;
// new callers should depend on jobs.RunRetentionLoop directly to avoid pulling
// the entire worker runtime dependency graph.
func RunJobRetentionLoop(ctx context.Context, log Logger, queue Queue, cfg config.WorkerJobRetentionConfig) {
	jobs.RunRetentionLoop(ctx, log, queue, jobs.RetentionConfig{
		Enabled:          cfg.Enabled,
		SucceededMaxAge:  cfg.SucceededMaxAge,
		DeadMaxAge:       cfg.DeadMaxAge,
		RunInterval:      cfg.RunInterval,
		DeleteBatchLimit: cfg.DeleteBatchLimit,
	})
}

func RunInvalidEventsRetentionLoop(ctx context.Context, log Logger, store InvalidEventRetentionStore, cfg config.WorkerInvalidEventRetentionConfig) {
	if !cfg.Enabled {
		log.Info("invalid_events_retention_disabled")
		return
	}
	if cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error("invalid_events_retention_invalid_config", "run_interval", cfg.RunInterval.String(), "delete_batch_limit", cfg.DeleteBatchLimit)
		return
	}
	log.Info(
		"invalid_events_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
		"payload_trim_enabled", cfg.PayloadTrim.Enabled,
		"payload_trim_max_age", cfg.PayloadTrim.MaxAge.String(),
		"payload_trim_batch_limit", cfg.PayloadTrim.BatchLimit,
	)

	ticker := time.NewTicker(cfg.RunInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().UTC().Add(-cfg.MaxAge)
			deleted, err := store.PurgeInvalidEventsOlderThan(ctx, cutoff, cfg.DeleteBatchLimit)
			if err != nil {
				metrics.IncRetentionPurgeRun("invalid_events", "error")
				log.Error("invalid_events_retention_purge_failed", "error", err)
				continue
			}
			metrics.IncRetentionPurgeRun("invalid_events", "ok")
			metrics.AddRetentionPurgedRows("invalid_events", deleted)
			if deleted > 0 {
				log.Info("invalid_events_retention_purged", "deleted", deleted, "cutoff", cutoff.Format(time.RFC3339))
			}
			if !cfg.PayloadTrim.Enabled {
				continue
			}

			trimCutoff := time.Now().UTC().Add(-cfg.PayloadTrim.MaxAge)
			trimmed, trimErr := store.TrimInvalidEventPayloadsOlderThan(ctx, trimCutoff, cfg.PayloadTrim.BatchLimit)
			if trimErr != nil {
				metrics.IncRetentionPurgeRun("invalid_events_payload", "error")
				log.Error("invalid_events_payload_trim_failed", "error", trimErr)
				continue
			}
			metrics.IncRetentionPurgeRun("invalid_events_payload", "ok")
			metrics.AddRetentionPurgedRows("invalid_events_payload", trimmed)
			if trimmed > 0 {
				log.Info("invalid_events_payload_trimmed", "trimmed", trimmed, "cutoff", trimCutoff.Format(time.RFC3339))
			}
		}
	}
}
