package jobs

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// replaceableRetentionTarget is the bounded metric label for superseded
// replaceable purge runs/rows (reuses the shared retention metric vectors in
// internal/metrics).
const replaceableRetentionTarget = "replaceable_events"

// ReplaceableRetentionPurger deletes a bounded batch of superseded raw
// replaceable events (kinds 0/3/10002). Defined as an interface so the loop can
// be unit tested without importing internal/store. Satisfied by
// *store.PostgresStore.
type ReplaceableRetentionPurger interface {
	PurgeSupersededReplaceableEvents(ctx context.Context, supersededBefore time.Time, deadGraceBefore time.Time, limit int) (int64, error)
}

// ReplaceableRetentionConfig is the narrow projection of
// config.WorkerReplaceableRetentionConfig the loop needs.
type ReplaceableRetentionConfig struct {
	Enabled          bool
	MinAge           time.Duration
	DeadGrace        time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunReplaceableRetentionLoop periodically purges raw replaceable events
// (kinds 0/3/10002) that have been strictly superseded by a newer winner and
// have been stable for at least MinAge, skipping events whose derivation is
// still in-flight (or recently dead within DeadGrace). It uses the same
// auto-pacing drain as the job/engagement retention loops: a saturated batch
// immediately re-runs after retentionCatchupPause until a batch comes back
// below the limit, so DeleteBatchLimit chunks work rather than capping
// throughput. Blocks until ctx is done.
func RunReplaceableRetentionLoop(ctx context.Context, log RetentionLogger, purger ReplaceableRetentionPurger, cfg ReplaceableRetentionConfig) {
	if !cfg.Enabled {
		log.Info("replaceable_retention_disabled")
		return
	}
	if cfg.MinAge <= 0 || cfg.DeadGrace <= 0 || cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"replaceable_retention_invalid_config",
			"min_age", cfg.MinAge.String(),
			"dead_grace", cfg.DeadGrace.String(),
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"replaceable_retention_enabled",
		"min_age", cfg.MinAge.String(),
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
		runReplaceableRetentionDrain(ctx, log, purger, cfg)
	}
}

func runReplaceableRetentionDrain(ctx context.Context, log RetentionLogger, purger ReplaceableRetentionPurger, cfg ReplaceableRetentionConfig) {
	consecutiveSaturated := 0
	for {
		now := time.Now().UTC()
		supersededBefore := now.Add(-cfg.MinAge)
		deadGraceBefore := now.Add(-cfg.DeadGrace)
		deleted, err := purger.PurgeSupersededReplaceableEvents(ctx, supersededBefore, deadGraceBefore, cfg.DeleteBatchLimit)
		if err != nil {
			metrics.IncRetentionPurgeRun(replaceableRetentionTarget, "error")
			log.Error("replaceable_retention_purge_failed", "error", err)
			return
		}
		metrics.IncRetentionPurgeRun(replaceableRetentionTarget, "ok")
		metrics.AddRetentionPurgedRows(replaceableRetentionTarget, deleted)
		if deleted > 0 {
			log.Info(
				"replaceable_retention_purged",
				"deleted", deleted,
				"superseded_before", supersededBefore.Format(time.RFC3339),
				"dead_grace_before", deadGraceBefore.Format(time.RFC3339),
			)
		}
		if int(deleted) < cfg.DeleteBatchLimit {
			return
		}
		consecutiveSaturated++
		if consecutiveSaturated%retentionCatchupReportEvery == 0 {
			log.Info(
				"replaceable_retention_catchup",
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
