package runtime

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/meili"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

// Overridable for tests (see refreshRelayWindowSnapshotsWithRetry).
var relayWindowSnapshotsRefreshRetryDelay = 5 * time.Second

// relayWindowSnapshotsRefreshInterval is how often the worker
// recomputes the homepage relay summary stats (24h / 7d windows +
// top-10 relays by activity) and overwrites
// relay_window_snapshots. The underlying queries take ~10s of CPU
// per run on production data, so 5 minutes is a good balance:
//   - Fresh enough that the homepage's "active authors / events"
//     numbers visibly track current network activity.
//   - Cheap enough that a full refresh is well under 1% of one core
//     in steady state, even with multiple worker replicas
//     duplicating the work (the upserts are idempotent).
const relayWindowSnapshotsRefreshInterval = 5 * time.Minute

// relayWindowSnapshotsRefreshTimeout caps how long a single refresh
// attempt may run (Go context). The seed in migration 000047 takes
// ~12s on production; under retention IO pressure the 7d
// COUNT(DISTINCT) over event_relays has been observed near ~40s once
// the per-statement Postgres budget is raised to match (see
// derivation.relayWindowSnapshotStatementTimeout). Bumped from the
// original 60s to 120s after 000061_domain_window_snapshots.sql added
// two more COUNT(DISTINCT)-shaped aggregates (top_domains_24h/7d),
// then to 300s after the 2026-08 relay-cap incident: an 8x ingest
// flood left a week of inflated raw rows in the 7d windows, and the
// refresh chain (rollup catchup + summary + languages + hashtags +
// domains) needs headroom above any single statement's 100s budget
// to finish while that data ages out. Every statement is still
// individually capped (SET LOCAL statement_timeout) and the phases
// commit separately, so a large Go budget only ever adds slack, and
// a 300s worst-case tick still finishes before the next 5-minute
// tick would pile up meaningfully.
const relayWindowSnapshotsRefreshTimeout = 300 * time.Second

// relayWindowSnapshotsRefreshAttempts is the number of tries per tick
// (initial + one retry on failure). Transient buffer-cache eviction
// from concurrent retention deletes often clears within a few seconds;
// retrying once avoids a full 5-minute staleness wait.
const relayWindowSnapshotsRefreshAttempts = 2

// RunRelayWindowSnapshotsLoop periodically refreshes the homepage
// relay summary snapshot. The homepage handler reads
// relay_window_snapshots with a sub-millisecond row lookup; without
// this loop those rows would be frozen at whatever the migration
// seeded.
//
// Why this is its own loop, not a sweeper
// ---------------------------------------
// Sweepers (author analytics, profile stats, meilisearch) drain a
// per-event pending queue produced by derive_event_bundle: the
// per-event upsert is cheap and the heavy work is pushed to the
// sweeper. The relay snapshot has no per-event dirtiness signal —
// a single new event_relays row barely changes any of the
// aggregates — so a fixed-interval refresh is the right shape.
//
// Failure handling
// ----------------
// On any error the previous snapshot row is left in place and the
// loop simply waits for the next tick. This means a transient DB
// problem causes the homepage to serve slightly older numbers, not
// to fail. Persistent failures show up as an old computed_at on
// /api/v1/discovery/home, as repeated error logs here, AND (as of
// the metrics.SetRelayWindowSnapshotAge call in
// refreshRelayWindowSnapshotsOnce) as the
// nostrmash_relay_window_snapshot_age_seconds gauge, which pages via
// the NostrMashRelayWindowSnapshotStale alert. Before that alert
// existed, this incident class (a stuck refresh loop, or a worker
// that stopped running entirely) had no active signal — it silently
// served 3-day-old homepage numbers until a user noticed.
func RunRelayWindowSnapshotsLoop(ctx context.Context, log Logger, handlers *derivation.Handlers) {
	if handlers == nil {
		log.Error("relay_window_snapshots_no_handlers")
		return
	}
	log.Info(
		"relay_window_snapshots_enabled",
		"interval", relayWindowSnapshotsRefreshInterval.String(),
		"timeout", relayWindowSnapshotsRefreshTimeout.String(),
	)
	// Fire one refresh immediately on startup so a worker restart
	// doesn't leave the homepage serving a stale snapshot for up to
	// the full refresh interval. The migration seeded the rows so
	// this is just keeping them current; we still want the very
	// first scheduled refresh to happen "soon" rather than 5
	// minutes from now.
	refreshRelayWindowSnapshotsOnce(ctx, log, handlers)

	ticker := time.NewTicker(relayWindowSnapshotsRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshRelayWindowSnapshotsOnce(ctx, log, handlers)
		}
	}
}

func refreshRelayWindowSnapshotsOnce(ctx context.Context, log Logger, handlers *derivation.Handlers) {
	// A panic anywhere in this tick (e.g. an unexpected row shape, a nil
	// pointer from a driver edge case) must not take down the entire
	// worker process: RunLifecycle spawns ~15 unrelated background loops
	// (retention, storage governor, meilisearch sync, ...) on the same
	// goroutine group, none of which should stop just because this one
	// tick had a bug. Recovering here keeps this loop's ticker alive for
	// the next interval instead of crashing the whole binary.
	defer func() {
		if r := recover(); r != nil {
			log.Error("relay_window_snapshots_refresh_panicked", "panic", r)
		}
	}()

	_ = refreshRelayWindowSnapshotsWithRetry(ctx, log, handlers.RefreshRelayWindowSnapshots)

	// Report the snapshot's actual staleness regardless of whether this
	// tick's refresh succeeded — a failing tick is exactly the case the
	// NostrMashRelayWindowSnapshotStale alert needs to catch, and reading
	// the DB row (rather than only advancing a metric on success) means
	// the gauge reflects the truth even across worker restarts.
	ageCtx, ageCancel := context.WithTimeout(ctx, 5*time.Second)
	defer ageCancel()
	age, ok, err := handlers.RelayWindowSnapshotAge(ageCtx)
	if err != nil {
		log.Error("relay_window_snapshots_age_query_failed", "error", err)
		return
	}
	if !ok {
		return
	}
	metrics.SetRelayWindowSnapshotAge(age.Seconds())
}

// refreshRelayWindowSnapshotsWithRetry runs refresh up to
// relayWindowSnapshotsRefreshAttempts times with a short delay between
// failures. Extracted so unit tests can exercise the retry path without a
// live DB.
func refreshRelayWindowSnapshotsWithRetry(
	ctx context.Context,
	log Logger,
	refresh func(context.Context) error,
) error {
	var lastErr error
	for attempt := 1; attempt <= relayWindowSnapshotsRefreshAttempts; attempt++ {
		runCtx, cancel := context.WithTimeout(ctx, relayWindowSnapshotsRefreshTimeout)
		started := time.Now()
		err := refresh(runCtx)
		cancel()
		if err == nil {
			log.Info(
				"relay_window_snapshots_refreshed",
				"duration_s", time.Since(started).Seconds(),
				"attempt", attempt,
			)
			return nil
		}
		lastErr = err
		log.Error(
			"relay_window_snapshots_refresh_failed",
			"error", err,
			"duration_s", time.Since(started).Seconds(),
			"attempt", attempt,
		)
		if attempt >= relayWindowSnapshotsRefreshAttempts || !shouldRetryRelayWindowSnapshotRefresh(ctx, err) {
			break
		}
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(relayWindowSnapshotsRefreshRetryDelay):
		}
	}
	return lastErr
}

func shouldRetryRelayWindowSnapshotRefresh(parent context.Context, err error) bool {
	if parent.Err() != nil || err == nil {
		return false
	}
	// Permanent configuration / wiring errors — retrying cannot help.
	if strings.Contains(err.Error(), "handlers are not initialized") {
		return false
	}
	return true
}

// RunMeilisearchStartupSync performs a one-shot reconciliation between
// PostgreSQL and Meilisearch in the background. It MUST NOT block the worker
// lifecycle: with hundreds of thousands of notes/profiles a full reindex can
// take many minutes, and during that time we still want claim loops, stale
// recovery, and the metrics endpoint to be running.
//
// Coordinates with the api service (which runs the same check on its own
// startup) via a Postgres advisory lock inside
// RunStartupFullSyncIfNeeded: when both restart together, only the
// instance that wins the lock actually streams the corpus into
// Meilisearch, so the two no longer double the load on an already
// resource-constrained Meilisearch instance.
func RunMeilisearchStartupSync(ctx context.Context, log Logger, client *meili.Client, pool *pgxpool.Pool) {
	if client == nil || !client.Enabled() || pool == nil {
		return
	}
	started := time.Now()
	stats, ran, syncErr := client.RunStartupFullSyncIfNeeded(ctx, pool, 1000)
	if syncErr != nil {
		log.Error("meilisearch_startup_sync_failed", "error", syncErr, "duration_s", time.Since(started).Seconds())
		return
	}
	if !ran {
		return
	}
	log.Info("meilisearch_indexes_stale", "action", "starting_full_sync")
	log.Info(
		"meilisearch_startup_sync_complete",
		"profiles", stats.Profiles,
		"notes", stats.Notes,
		"documents", stats.Documents,
		"duration_s", time.Since(started).Seconds(),
	)
}

func RunStaleRecoveryLoop(ctx context.Context, log Logger, queue Queue, workerPool string, cfg config.WorkerJobRecoveryConfig) {
	if cfg.StaleRecoveryInterval <= 0 || cfg.RunningTimeout <= 0 || cfg.StaleRecoveryBatchLimit <= 0 {
		log.Error(
			"stale_recovery_invalid_config",
			"worker_pool", workerPool,
			"running_timeout", cfg.RunningTimeout.String(),
			"stale_recovery_interval", cfg.StaleRecoveryInterval.String(),
			"stale_recovery_batch_limit", cfg.StaleRecoveryBatchLimit,
		)
		return
	}
	log.Info(
		"stale_recovery_enabled",
		"worker_pool", workerPool,
		"running_timeout", cfg.RunningTimeout.String(),
		"stale_recovery_interval", cfg.StaleRecoveryInterval.String(),
		"stale_recovery_batch_limit", cfg.StaleRecoveryBatchLimit,
	)
	ticker := time.NewTicker(cfg.StaleRecoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			olderThan := time.Now().UTC().Add(-cfg.RunningTimeout)
			started := time.Now()
			result, err := queue.RecoverStaleRunningJobs(ctx, workerPool, olderThan, cfg.StaleRecoveryBatchLimit)
			recoveryResult := "ok"
			if err != nil {
				recoveryResult = "error"
			}
			metrics.ObserveStaleRecoveryDuration(workerPool, recoveryResult, time.Since(started))
			if err != nil {
				log.Error("stale_recovery_failed", "worker_pool", workerPool, "error", err)
				continue
			}
			metrics.AddStaleRecoveryRecovered(workerPool, result.Recovered)
			metrics.AddStaleRecoveryDeadLettered(workerPool, result.DeadLettered)
			if result.Recovered > 0 || result.DeadLettered > 0 {
				log.Info(
					"stale_recovery_completed",
					"worker_pool", workerPool,
					"recovered", result.Recovered,
					"dead_lettered", result.DeadLettered,
					"older_than", olderThan.Format(time.RFC3339),
				)
			}
		}
	}
}

func RunQueueAndRebuildMetricsReporter(ctx context.Context, log Logger, pool *pgxpool.Pool, workerPools []string, every time.Duration) {
	if pool == nil || every <= 0 {
		return
	}
	pools := make([]string, 0, len(workerPools))
	seen := make(map[string]struct{}, len(workerPools))
	for _, name := range workerPools {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		pools = append(pools, name)
	}
	if len(pools) == 0 {
		pools = []string{jobs.WorkerPoolDefault}
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var maxAge float64
			for _, workerPool := range pools {
				var oldestPending *float64
				if err := pool.QueryRow(ctx, `
					SELECT EXTRACT(EPOCH FROM (now() - MIN(run_after)))
					FROM jobs
					WHERE status = 'pending'
					  AND worker_pool = $1
				`, workerPool).Scan(&oldestPending); err != nil {
					log.Error("queue_backlog_metrics_query_failed", "worker_pool", workerPool, "error", err)
					continue
				}
				if oldestPending != nil && *oldestPending > maxAge {
					maxAge = *oldestPending
				}
			}
			metrics.SetWorkerQueueBacklogOldestPendingAge(maxAge)

			var rebuildCount float64
			if err := pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM projection_rebuild_runs
				WHERE status = 'running'
			`).Scan(&rebuildCount); err != nil {
				log.Error("rebuild_active_count_query_failed", "error", err)
			} else {
				metrics.SetRebuildRunsActive(rebuildCount)
			}

			var oldestActive *float64
			if err := pool.QueryRow(ctx, `
				SELECT EXTRACT(EPOCH FROM (now() - MIN(COALESCE(started_at, created_at))))
				FROM projection_rebuild_runs
				WHERE status = 'running'
			`).Scan(&oldestActive); err != nil {
				log.Error("rebuild_active_age_query_failed", "error", err)
			} else if oldestActive != nil {
				metrics.SetRebuildActiveOldestAge(*oldestActive)
			} else {
				metrics.SetRebuildActiveOldestAge(0)
			}
		}
	}
}
