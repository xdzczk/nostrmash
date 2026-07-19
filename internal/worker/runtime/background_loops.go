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
// may run. The seed in migration 000047 takes ~12s on production,
// and pathological cases (e.g. autovacuum holding locks during a
// table bloat) could push it higher; 60s gives generous headroom
// without ever permanently wedging the loop on a single bad run.
const relayWindowSnapshotsRefreshTimeout = 60 * time.Second

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
// /api/v1/discovery/home and as repeated error logs here.
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
	runCtx, cancel := context.WithTimeout(ctx, relayWindowSnapshotsRefreshTimeout)
	defer cancel()
	started := time.Now()
	if err := handlers.RefreshRelayWindowSnapshots(runCtx); err != nil {
		log.Error(
			"relay_window_snapshots_refresh_failed",
			"error", err,
			"duration_s", time.Since(started).Seconds(),
		)
		return
	}
	log.Info(
		"relay_window_snapshots_refreshed",
		"duration_s", time.Since(started).Seconds(),
	)
}

// RunMeilisearchStartupSync performs a one-shot reconciliation between
// PostgreSQL and Meilisearch in the background. It MUST NOT block the worker
// lifecycle: with hundreds of thousands of notes/profiles a full reindex can
// take many minutes, and during that time we still want claim loops, stale
// recovery, and the metrics endpoint to be running.
func RunMeilisearchStartupSync(ctx context.Context, log Logger, client *meili.Client, pool *pgxpool.Pool) {
	if client == nil || !client.Enabled() || pool == nil {
		return
	}
	needsSync, syncCheckErr := client.NeedsSync(ctx, pool)
	if syncCheckErr != nil {
		log.Error("meilisearch_sync_check", "error", syncCheckErr)
		return
	}
	if !needsSync {
		return
	}
	log.Info("meilisearch_indexes_stale", "action", "starting_full_sync")
	started := time.Now()
	stats, syncErr := client.FullSync(ctx, pool, 1000)
	if syncErr != nil {
		log.Error("meilisearch_startup_sync_failed", "error", syncErr, "duration_s", time.Since(started).Seconds())
		return
	}
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
