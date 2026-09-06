package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/meili"
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
			processed, err := drainMeilisearchSyncBatch(ctx, handlers, cfg)
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
				processed, err = drainMeilisearchSyncBatch(ctx, handlers, cfg)
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

// meilisearchIndexRetentionInterval is how often aged note documents are
// purged from the Meilisearch notes index. The purge is one delete-by-filter
// task per tick, so a few ticks a day is plenty — the horizon itself
// (indexedNotesMaxAge) moves only with wall-clock time.
const meilisearchIndexRetentionInterval = 6 * time.Hour

// RunMeilisearchIndexRetentionLoop keeps the notes index bounded to its
// designed age window. FullSync never indexes notes older than the horizon,
// but incremental syncs add every fresh note and nothing deleted them as
// they aged, so the live index grew without bound between full rebuilds and
// Meilisearch's per-commit CPU cost (proportional to index size) grew with
// it. Runs an immediate purge on startup, then ticks.
func RunMeilisearchIndexRetentionLoop(ctx context.Context, log Logger, client *meili.Client) {
	if client == nil || !client.Enabled() {
		return
	}
	purge := func() {
		started := time.Now()
		deleted, err := client.PurgeAgedNotes(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("meilisearch_index_retention_failed", "error", err, "duration_s", time.Since(started).Seconds())
			return
		}
		log.Info(
			"meilisearch_index_retention_purged",
			"deleted_documents", deleted,
			"duration_s", time.Since(started).Seconds(),
		)
	}
	purge()
	ticker := time.NewTicker(meilisearchIndexRetentionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			purge()
		}
	}
}

// drainMeilisearchSyncBatch wraps DrainPendingMeilisearchSyncBatch with a
// hard per-call deadline (cfg.BatchTimeout) covering the whole claim+sync
// call, not just the Meilisearch HTTP portion that meili.SyncEventsBatch
// already bounds internally. See WorkerMeilisearchSweeperConfig's doc
// comment: without this, a stall anywhere else in the call path (e.g. the
// claim transaction) has no backstop and can wedge the goroutine
// indefinitely with no error ever logged. A zero/negative BatchTimeout
// disables the wrapper (used by tests that don't set it).
func drainMeilisearchSyncBatch(ctx context.Context, handlers *derivation.Handlers, cfg config.WorkerMeilisearchSweeperConfig) (int, error) {
	return runWithBatchTimeout(ctx, cfg.BatchTimeout, func(callCtx context.Context) (int, error) {
		return handlers.DrainPendingMeilisearchSyncBatch(callCtx, cfg.BatchSize)
	})
}

// runWithBatchTimeout bounds fn by a real wall-clock timeout when
// timeout > 0. fn runs in its own goroutine so the deadline is enforced
// even if fn (or something deep in its call graph) never checks
// ctx.Done() — passing a context.WithTimeout ctx into fn is not
// sufficient on its own, since context cancellation only takes effect at
// points that actively select on it, and the production hang this guards
// against was precisely a call that never returned despite an
// already-canceled context reaching it.
//
// On timeout, fn's goroutine is left running and its result is discarded
// when it eventually completes (resCh is buffered so that send never
// blocks); this trades a bounded goroutine leak per stall for guaranteeing
// the sweeper loop itself is never wedged. A zero/negative timeout runs fn
// on the caller's context unmodified with no goroutine indirection.
func runWithBatchTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context) (int, error)) (int, error) {
	if timeout <= 0 {
		return fn(ctx)
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		processed int
		err       error
	}
	resCh := make(chan result, 1)
	go func() {
		processed, err := fn(callCtx)
		resCh <- result{processed, err}
	}()

	select {
	case res := <-resCh:
		return res.processed, res.err
	case <-callCtx.Done():
		return 0, fmt.Errorf("meilisearch sweeper batch exceeded %s hard timeout: %w", timeout, callCtx.Err())
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
