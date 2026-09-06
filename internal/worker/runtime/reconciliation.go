package runtime

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

// RunIncrementalStatsReconciliationLoop is the correctness backstop for the
// incremental author/profile stats design (see
// docs/design/incremental-author-stats.md). On each tick it full-recomputes
// a sample of pubkeys (read-only) and compares against the incrementally
// maintained values, logging and incrementing a metric for every mismatch
// found. A single goroutine is sufficient: this is deliberately low
// priority, low-frequency background work, not steady-state load.
func RunIncrementalStatsReconciliationLoop(
	ctx context.Context,
	log Logger,
	handlers *derivation.Handlers,
	cfg config.WorkerIncrementalStatsReconciliationConfig,
) {
	if handlers == nil {
		log.Error("incremental_stats_reconciliation_no_handlers")
		return
	}
	if !cfg.Enabled || cfg.Interval <= 0 || cfg.SampleSize <= 0 {
		log.Info("incremental_stats_reconciliation_disabled")
		return
	}
	log.Info(
		"incremental_stats_reconciliation_enabled",
		"interval", cfg.Interval.String(),
		"sample_size", cfg.SampleSize,
	)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runIncrementalStatsReconciliationOnce(ctx, log, handlers, cfg.SampleSize)
		}
	}
}

func runIncrementalStatsReconciliationOnce(ctx context.Context, log Logger, handlers *derivation.Handlers, sampleSize int) {
	// A panic here (e.g. an unexpected row shape) must not take down the
	// whole worker process — see the identical rationale on
	// refreshRelayWindowSnapshotsOnce in background_loops.go.
	defer func() {
		if r := recover(); r != nil {
			log.Error("incremental_stats_reconciliation_panicked", "panic", r)
		}
	}()

	started := time.Now()
	report, err := handlers.ReconcileIncrementalAuthorStatsSample(ctx, sampleSize)
	if err != nil {
		metrics.ObserveIncrementalStatsReconciliationRun("error", 0, time.Since(started))
		log.Error("incremental_stats_reconciliation_failed", "error", err, "duration_s", time.Since(started).Seconds())
		return
	}
	metrics.ObserveIncrementalStatsReconciliationRun("ok", report.SampledPubkeys, time.Since(started))

	for _, mismatch := range report.Mismatches {
		metrics.IncIncrementalStatsReconciliationMismatch(mismatch.Projection, mismatch.Field)
		log.Error(
			"incremental_stats_reconciliation_mismatch",
			"pubkey", mismatch.Pubkey,
			"projection", mismatch.Projection,
			"field", mismatch.Field,
			"incremental_value", mismatch.Incremental,
			"recomputed_value", mismatch.Recomputed,
		)
	}

	// Self-heal drifted pubkeys via the projections' own rebuild paths so a
	// mismatch is logged once and repaired, not re-logged on every pass.
	healed := 0
	for _, heal := range handlers.HealReconciliationMismatches(ctx, report.Mismatches) {
		if heal.Err != nil {
			metrics.IncIncrementalStatsReconciliationHeal(heal.Action, "error")
			log.Error(
				"incremental_stats_reconciliation_heal_failed",
				"pubkey", heal.Pubkey,
				"action", heal.Action,
				"error", heal.Err,
			)
			continue
		}
		metrics.IncIncrementalStatsReconciliationHeal(heal.Action, "ok")
		healed++
		log.Info(
			"incremental_stats_reconciliation_healed",
			"pubkey", heal.Pubkey,
			"action", heal.Action,
		)
	}

	log.Info(
		"incremental_stats_reconciliation_completed",
		"sampled_pubkeys", report.SampledPubkeys,
		"mismatches", len(report.Mismatches),
		"healed", healed,
		"duration_s", time.Since(started).Seconds(),
	)
}
