package jobs

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// RetentionLogger is the minimal logger interface the retention loop needs.
// Both internal/worker/runtime.Logger and *slog.Logger satisfy it, so callers
// can pass either without dragging the heavier worker runtime dependency
// graph into lighter binaries (notably trust_worker).
type RetentionLogger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// RetentionPurger is the slice of *Queue used by the retention loop. Defined
// as an interface so tests can fake it and so callers do not need to drag in
// a *pgxpool.Pool just to wire the loop.
type RetentionPurger interface {
	PurgeTerminalJobs(ctx context.Context, succeededBefore, deadBefore time.Time, limit int) (int64, error)
}

// RetentionConfig is the small projection of WorkerJobRetentionConfig that
// the retention loop actually needs. Keeping it narrow avoids importing
// internal/config from internal/jobs (which would invert the natural module
// direction).
type RetentionConfig struct {
	Enabled          bool
	SucceededMaxAge  time.Duration
	DeadMaxAge       time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// retentionCatchupPause is the brief courtesy delay between consecutive
// saturated purge batches. Long enough to release CPU and let interactive
// queries grab the partial index between batches, short enough that backlogs
// drain at roughly disk speed instead of `RunInterval` speed.
//
// Exposed as a package-level var (not a constant) so tests can shrink it
// without making it a public knob — operators should never tune this.
var retentionCatchupPause = 100 * time.Millisecond

// retentionCatchupReportEvery is the cadence (in consecutive saturated
// batches) at which the loop emits a `job_retention_catchup` log line. The
// goal is "operator notices a sustained backlog burn-down", not per-batch
// chatter, so the default is intentionally generous.
const retentionCatchupReportEvery = 50

// RunRetentionLoop periodically purges terminal `jobs` rows whose finished_at
// is older than the configured cutoff. Blocks until ctx is done. Safe to call
// from multiple workers — the underlying DELETE batch is bounded and ordered,
// so concurrent purges just race for the next batch without corruption.
//
// The loop is auto-paced: when a batch returns saturated (`deleted >=
// DeleteBatchLimit`) it immediately re-runs after `retentionCatchupPause`,
// repeating until a batch comes back below the limit. Only then does it sleep
// `RunInterval` until the next scheduled tick. This makes `DeleteBatchLimit`
// behave like a *per-batch chunking knob* rather than a *throughput ceiling*,
// so an operator-induced backlog (or a workload spike) drains at disk speed
// without anyone having to retune the env defaults.
func RunRetentionLoop(ctx context.Context, log RetentionLogger, queue RetentionPurger, cfg RetentionConfig) {
	if !cfg.Enabled {
		log.Info("job_retention_disabled")
		return
	}
	if cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"job_retention_invalid_config",
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"job_retention_enabled",
		"succeeded_max_age", cfg.SucceededMaxAge.String(),
		"dead_max_age", cfg.DeadMaxAge.String(),
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
		runRetentionDrain(ctx, log, queue, cfg)
	}
}

// runRetentionDrain executes one scheduled retention cycle, which is itself
// allowed to perform multiple back-to-back DELETE batches as long as each
// batch comes back saturated. Returns when (a) a batch is below the limit,
// (b) PurgeTerminalJobs errors, or (c) ctx is cancelled.
func runRetentionDrain(ctx context.Context, log RetentionLogger, queue RetentionPurger, cfg RetentionConfig) {
	consecutiveSaturated := 0
	for {
		now := time.Now().UTC()
		succeededBefore := now.Add(-cfg.SucceededMaxAge)
		deadBefore := now.Add(-cfg.DeadMaxAge)
		deleted, err := queue.PurgeTerminalJobs(ctx, succeededBefore, deadBefore, cfg.DeleteBatchLimit)
		if err != nil {
			metrics.IncRetentionPurgeRun("jobs_terminal", "error")
			log.Error("job_retention_purge_failed", "error", err)
			return
		}
		metrics.IncRetentionPurgeRun("jobs_terminal", "ok")
		metrics.AddRetentionPurgedRows("jobs_terminal", deleted)
		if deleted > 0 {
			log.Info(
				"job_retention_purged",
				"deleted", deleted,
				"succeeded_before", succeededBefore.Format(time.RFC3339),
				"dead_before", deadBefore.Format(time.RFC3339),
			)
		}
		// Below-limit batch == queue is caught up for now; let the outer
		// ticker schedule the next cycle.
		if int(deleted) < cfg.DeleteBatchLimit {
			return
		}
		consecutiveSaturated++
		if consecutiveSaturated%retentionCatchupReportEvery == 0 {
			log.Info(
				"job_retention_catchup",
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
