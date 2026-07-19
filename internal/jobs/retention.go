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

	runRetentionTicker(ctx, cfg.RunInterval, jobsRetentionDrain(log, queue, cfg))
}

// retentionDrain is the shared auto-pacing engine behind every retention loop.
// A drain runs purge batches back-to-back while each returns saturated
// (deleted >= batchLimit), pausing retentionCatchupPause between batches and
// emitting standardized purge/catchup/error logs plus retention metrics. Each
// loop supplies a purge closure (which computes cutoffs, performs the delete,
// and returns the rows deleted plus fields to log) and its log/metric identity,
// so the ticker + catchup + metrics mechanics live in exactly one place.
type retentionDrain struct {
	log          RetentionLogger
	metricTarget string
	batchLimit   int
	purgedEvent  string // log event on a non-empty purge, e.g. "job_retention_purged"
	failedEvent  string // log event on purge error, e.g. "job_retention_purge_failed"
	catchupEvent string // log event on sustained catchup, e.g. "job_retention_catchup"
	// staticFields are prepended to every log line (e.g. {"target", ...} for
	// loops that drain multiple scopes through the same engine). May be nil.
	staticFields []any
	// purge runs exactly one batch and returns the rows deleted plus structured
	// fields describing the cutoffs used, included on a non-empty purge line.
	purge func(ctx context.Context) (deleted int64, purgedFields []any, err error)
}

// run executes one scheduled retention cycle. It returns when a batch comes
// back below the limit, the purge errors, or ctx is cancelled.
func (d retentionDrain) run(ctx context.Context) {
	consecutiveSaturated := 0
	for {
		deleted, purgedFields, err := d.purge(ctx)
		if err != nil {
			metrics.IncRetentionPurgeRun(d.metricTarget, "error")
			d.log.Error(d.failedEvent, d.withStatic("error", err)...)
			return
		}
		metrics.IncRetentionPurgeRun(d.metricTarget, "ok")
		metrics.AddRetentionPurgedRows(d.metricTarget, deleted)
		if deleted > 0 {
			d.log.Info(d.purgedEvent, d.withStatic(append([]any{"deleted", deleted}, purgedFields...)...)...)
		}
		if int(deleted) < d.batchLimit {
			return
		}
		consecutiveSaturated++
		if consecutiveSaturated%retentionCatchupReportEvery == 0 {
			d.log.Info(d.catchupEvent, d.withStatic("consecutive_full_batches", consecutiveSaturated, "delete_batch_limit", d.batchLimit)...)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retentionCatchupPause):
		}
	}
}

// withStatic prepends the drain's static log fields to per-call fields.
func (d retentionDrain) withStatic(extra ...any) []any {
	if len(d.staticFields) == 0 {
		return extra
	}
	out := make([]any, 0, len(d.staticFields)+len(extra))
	out = append(out, d.staticFields...)
	out = append(out, extra...)
	return out
}

// runRetentionTicker schedules one or more drains on a shared interval ticker,
// running every drain per tick until ctx is cancelled. Multiple drains model
// loops that groom several scopes on the same cadence.
func runRetentionTicker(ctx context.Context, runInterval time.Duration, drains ...retentionDrain) {
	ticker := time.NewTicker(runInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, drain := range drains {
			if ctx.Err() != nil {
				return
			}
			drain.run(ctx)
		}
	}
}

// jobsRetentionDrain builds the terminal-jobs purge drain.
func jobsRetentionDrain(log RetentionLogger, queue RetentionPurger, cfg RetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: "jobs_terminal",
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "job_retention_purged",
		failedEvent:  "job_retention_purge_failed",
		catchupEvent: "job_retention_catchup",
		purge: func(ctx context.Context) (int64, []any, error) {
			now := time.Now().UTC()
			succeededBefore := now.Add(-cfg.SucceededMaxAge)
			deadBefore := now.Add(-cfg.DeadMaxAge)
			deleted, err := queue.PurgeTerminalJobs(ctx, succeededBefore, deadBefore, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, []any{
				"succeeded_before", succeededBefore.Format(time.RFC3339),
				"dead_before", deadBefore.Format(time.RFC3339),
			}, nil
		},
	}
}
