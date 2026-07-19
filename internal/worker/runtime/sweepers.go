package runtime

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

// RunAuthorAnalyticsSweeperLoop drains the
// pending_author_analytics_recomputes queue produced by
// derive_event_bundle. It plays the role of "deferred per-author
// projection rebuild" so the per-event bundle stays fast even for hot
// pubkeys receiving high-frequency reactions/reposts/zaps.
//
// Multiple sweeper goroutines can run in parallel — claims use FOR
// UPDATE SKIP LOCKED so they never block each other, and the per-pubkey
// advisory lock inside projectAuthorAnalyticsForPubkey serializes any
// remaining cross-worker conflicts (which should be rare given each
// pubkey is claimed by exactly one sweeper at a time).
func RunAuthorAnalyticsSweeperLoop(
	ctx context.Context,
	log Logger,
	handlers *derivation.Handlers,
	cfg config.WorkerAuthorAnalyticsSweeperConfig,
	workerIdx int,
) {
	if handlers == nil {
		log.Error("author_analytics_sweeper_no_handlers", "worker_idx", workerIdx)
		return
	}
	if !cfg.Enabled || cfg.Interval <= 0 || cfg.BatchSize <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			started := time.Now()
			processed, err := handlers.DrainPendingAuthorAnalyticsBatchWithTimeout(ctx, cfg.BatchSize, cfg.RebuildTimeout)
			outcome := "ok"
			if err != nil {
				outcome = "error"
				log.Error(
					"author_analytics_sweeper_batch_failed",
					"worker_idx", workerIdx,
					"processed", processed,
					"error", err,
				)
			}
			metrics.ObserveAuthorAnalyticsSweeperBatch(outcome, processed, time.Since(started))
			// When the queue is hot, drain back-to-back without sleeping until
			// a batch returns fewer rows than the limit. This lets the sweeper
			// catch up after a burst without waiting cfg.Interval between
			// every batch.
			for err == nil && processed >= cfg.BatchSize {
				select {
				case <-ctx.Done():
					return
				default:
				}
				started = time.Now()
				processed, err = handlers.DrainPendingAuthorAnalyticsBatchWithTimeout(ctx, cfg.BatchSize, cfg.RebuildTimeout)
				outcome = "ok"
				if err != nil {
					outcome = "error"
					log.Error(
						"author_analytics_sweeper_batch_failed",
						"worker_idx", workerIdx,
						"processed", processed,
						"error", err,
					)
				}
				metrics.ObserveAuthorAnalyticsSweeperBatch(outcome, processed, time.Since(started))
			}
		}
	}
}

// RunMeilisearchSweeperLoop drains the pending_meilisearch_syncs queue
// produced by derive_event_bundle. It plays the role of "deferred search
// index update" so per-event bundle latency is not dominated by the
// 30-second per-event Meili sync timeout.
//
// Multiple sweeper goroutines can run in parallel — claims use FOR
// UPDATE SKIP LOCKED so they never block each other. The per-event sync
// is idempotent (Meilisearch upserts), so a duplicate claim from a race
// is correctness-safe, just slightly wasteful.
func RunMeilisearchSweeperLoop(
	ctx context.Context,
	log Logger,
	handlers *derivation.Handlers,
	cfg config.WorkerMeilisearchSweeperConfig,
	workerIdx int,
) {
	if handlers == nil {
		log.Error("meilisearch_sweeper_no_handlers", "worker_idx", workerIdx)
		return
	}
	if !cfg.Enabled || cfg.Interval <= 0 || cfg.BatchSize <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// A single sweeper goroutine publishes the backlog/lag SLO gauge so
			// the N goroutines don't all issue the same aggregate query each tick.
			if workerIdx == 0 {
				reportMeilisearchBacklogGauge(ctx, log, handlers)
			}
			started := time.Now()
			processed, err := handlers.DrainPendingMeilisearchSyncBatch(ctx, cfg.BatchSize)
			outcome := "ok"
			if err != nil {
				outcome = "error"
				log.Error(
					"meilisearch_sweeper_batch_failed",
					"worker_idx", workerIdx,
					"processed", processed,
					"error", err,
				)
			}
			metrics.ObserveMeilisearchSweeperBatch(outcome, processed, time.Since(started))
			// Drain back-to-back when the queue is hot.
			for err == nil && processed >= cfg.BatchSize {
				select {
				case <-ctx.Done():
					return
				default:
				}
				started = time.Now()
				processed, err = handlers.DrainPendingMeilisearchSyncBatch(ctx, cfg.BatchSize)
				outcome = "ok"
				if err != nil {
					outcome = "error"
					log.Error(
						"meilisearch_sweeper_batch_failed",
						"worker_idx", workerIdx,
						"processed", processed,
						"error", err,
					)
				}
				metrics.ObserveMeilisearchSweeperBatch(outcome, processed, time.Since(started))
			}
		}
	}
}

// reportMeilisearchBacklogGauge publishes the pending Meilisearch sync backlog
// and oldest-entry age to Prometheus. Failures are logged and swallowed so a
// transient DB hiccup never wedges the sweeper.
func reportMeilisearchBacklogGauge(ctx context.Context, log Logger, handlers *derivation.Handlers) {
	backlog, oldestAge, err := handlers.PendingMeilisearchSyncStats(ctx)
	if err != nil {
		log.Error("meilisearch_sweeper_backlog_gauge_failed", "error", err)
		return
	}
	metrics.SetMeilisearchSyncBacklog(backlog, oldestAge.Seconds())
}

// RunProfileStatsSweeperLoop drains the
// pending_profile_stats_recomputes queue produced by
// derive_event_bundle. It runs ProjectProfilePublicStats and
// ProjectProfileDiscoveryStats out-of-band so the bundle no longer
// pays the per-pubkey advisory-lock + multi-second aggregate cost
// inline.
//
// Multiple sweeper goroutines can run in parallel — claims use FOR
// UPDATE SKIP LOCKED so they never block each other. Each pubkey is
// claimed by exactly one sweeper at a time, so cross-worker conflict
// on the per-pubkey advisory locks is rare.
func RunProfileStatsSweeperLoop(
	ctx context.Context,
	log Logger,
	handlers *derivation.Handlers,
	cfg config.WorkerProfileStatsSweeperConfig,
	workerIdx int,
) {
	if handlers == nil {
		log.Error("profile_stats_sweeper_no_handlers", "worker_idx", workerIdx)
		return
	}
	if !cfg.Enabled || cfg.Interval <= 0 || cfg.BatchSize <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			started := time.Now()
			processed, err := handlers.DrainPendingProfileStatsBatch(ctx, cfg.BatchSize)
			outcome := "ok"
			if err != nil {
				outcome = "error"
				log.Error(
					"profile_stats_sweeper_batch_failed",
					"worker_idx", workerIdx,
					"processed", processed,
					"error", err,
				)
			}
			metrics.ObserveProfileStatsSweeperBatch(outcome, processed, time.Since(started))
			// Drain back-to-back when the queue is hot.
			for err == nil && processed >= cfg.BatchSize {
				select {
				case <-ctx.Done():
					return
				default:
				}
				started = time.Now()
				processed, err = handlers.DrainPendingProfileStatsBatch(ctx, cfg.BatchSize)
				outcome = "ok"
				if err != nil {
					outcome = "error"
					log.Error(
						"profile_stats_sweeper_batch_failed",
						"worker_idx", workerIdx,
						"processed", processed,
						"error", err,
					)
				}
				metrics.ObserveProfileStatsSweeperBatch(outcome, processed, time.Since(started))
			}
		}
	}
}
