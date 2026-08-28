package apptrustworker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/runtimebootstrap"
	"github.com/xdzczk/nostrmash/internal/store"
	storetrust "github.com/xdzczk/nostrmash/internal/store/trust"
	"github.com/xdzczk/nostrmash/internal/trust"
)

// BuildInfo carries ldflags-injected build metadata from the binary entrypoint.
type BuildInfo struct {
	Version string
	Commit  string
	Time    string
}

// Run is the trust_worker composition root: it loads config, wires the trust
// runtime and background loops, and blocks on the claim loop until ctx is
// cancelled. It returns a non-nil error on fatal startup failures instead of
// calling os.Exit so the thin cmd entrypoint owns process termination.
func Run(ctx context.Context, log *slog.Logger, build BuildInfo) error {
	cfg, err := config.LoadTrustWorker()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	pool, err := store.OpenPool(ctx, cfg.Shared.Database.URL, cfg.Shared.Database.MaxConns)
	if err != nil {
		return fmt.Errorf("db_connect: %w", err)
	}
	defer pool.Close()
	metrics.RegisterDBPool(pool)

	var redisClient *redis.Client
	if cfg.EnableRedisSync {
		redisOpts, err := redis.ParseURL(cfg.Redis.URL)
		if err != nil {
			return fmt.Errorf("redis_parse_url: %w", err)
		}
		redisClient = redis.NewClient(redisOpts)
		defer func() { _ = redisClient.Close() }()
		if err := redisClient.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis_ping: %w", err)
		}
	}

	appVersion := runtimebootstrap.ResolveAppVersion()
	version := resolveBuildVersion(build.Version, appVersion)
	if err := runtimebootstrap.InitTracing(ctx, log, cfg.Shared.ServiceName, "trust_worker", version, cfg.Shared.Environment); err != nil {
		return fmt.Errorf("tracing_init: %w", err)
	}
	defer runtimebootstrap.ShutdownTracing(log)
	runtimebootstrap.RegisterBuildAndDeployment(
		log,
		"trust_worker",
		cfg.Shared.ServiceName,
		cfg.Shared.Environment,
		version,
		build.Commit,
		build.Time,
	)

	if err := store.Migrate(ctx, pool, appVersion); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	log.Info(
		"trust_worker_startup_mode",
		"redis_sync_enabled", cfg.EnableRedisSync,
		"score_compute_enabled", cfg.EnableScoreCompute,
		"neighborhoods_enabled", cfg.EnableNeighborhoods,
		"seed_teleport_enabled", cfg.EnableSeedTeleport,
		"mode", trustWorkerModeLabel(cfg.EnableRedisSync, cfg.EnableScoreCompute),
	)

	st := store.NewPostgresStore(pool)
	// Reconcile configured seeds into trust_seeds so the trust graph snapshot
	// (and downstream ingest gate) have BFS roots. Non-fatal: a transient DB
	// error here should not crash the worker; the next restart retries.
	if err := reconcileTrustSeeds(ctx, log, st, cfg.Shared.TrustPolicy.SeedPubkeys); err != nil {
		log.Error("trust_seed_reconcile_failed", "error", err)
	}

	queue := jobs.NewQueue(pool)
	runtime := trust.NewRuntimeWithRedis(
		pool,
		redisClient,
		cfg.Redis.KeyPrefix,
		cfg.EnableRedisSync,
		cfg.EnableScoreCompute,
	).WithNeighborhoods(
		cfg.EnableNeighborhoods,
		cfg.NeighborhoodMaxMembers,
		cfg.Shared.TrustPolicy.MaxHops,
	).WithInteractionGraph(cfg.EnableInteractionGraph).WithSeedTeleport(cfg.EnableSeedTeleport)
	workerID := resolveWorkerID()

	runtimebootstrap.StartMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	runtimebootstrap.StartDebugEndpoint(ctx, log, cfg.Shared.Observability.DebugAddr)
	go runTrustMetricsReporter(ctx, log, pool, 30*time.Second)
	go runStaleRecoveryLoop(ctx, log, queue, jobs.WorkerPoolTrust, cfg.JobRecovery)
	go jobs.RunRetentionLoop(ctx, log, queue, jobs.RetentionConfig{
		Enabled:          cfg.JobRetention.Enabled,
		SucceededMaxAge:  cfg.JobRetention.SucceededMaxAge,
		DeadMaxAge:       cfg.JobRetention.DeadMaxAge,
		RunInterval:      cfg.JobRetention.RunInterval,
		DeleteBatchLimit: cfg.JobRetention.DeleteBatchLimit,
	})
	go jobs.RunRowCountMetricsReporter(ctx, log, pool, 60*time.Second)
	go runTrustGraphSnapshotRefreshLoop(ctx, log, st, cfg.Shared.TrustPolicy.MaxHops, cfg.GraphSnapshotRefreshInterval)
	if cfg.EnableScoreCompute {
		go runTrustRunSchedulerLoop(ctx, log, pool, runtime, cfg.RunSchedulerInterval)
	}

	runClaimLoop(
		ctx,
		log,
		queue,
		workerID,
		jobs.WorkerPoolTrust,
		cfg.ClaimBatchSize,
		cfg.Concurrency,
		cfg.PollInterval,
		cfg.RetryDelay,
		runtime.ProcessJob,
	)
	return nil
}

func runStaleRecoveryLoop(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, queue workerQueue, workerPool string, cfg config.WorkerJobRecoveryConfig) {
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

func runTrustMetricsReporter(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, pool *pgxpool.Pool, every time.Duration) {
	if every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var oldestPending *float64
			if err := pool.QueryRow(ctx, `
				SELECT EXTRACT(EPOCH FROM (now() - MIN(run_after)))
				FROM jobs
				WHERE status = 'pending' AND worker_pool = 'trust'
			`).Scan(&oldestPending); err != nil {
				log.Error("trust_queue_backlog_query_failed", "error", err)
			} else if oldestPending != nil {
				metrics.SetTrustQueueBacklogOldestPendingAge(*oldestPending)
			}

			var activeRuns float64
			if err := pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM trust_runs
				WHERE status = 'running'
			`).Scan(&activeRuns); err != nil {
				log.Error("trust_active_runs_query_failed", "error", err)
			} else {
				metrics.SetTrustRunsActive(activeRuns)
			}

			var oldestActiveAge *float64
			if err := pool.QueryRow(ctx, `
				SELECT EXTRACT(EPOCH FROM (now() - MIN(started_at)))
				FROM trust_runs
				WHERE status = 'running' AND started_at IS NOT NULL
			`).Scan(&oldestActiveAge); err != nil {
				log.Error("trust_active_run_age_query_failed", "error", err)
			} else if oldestActiveAge != nil {
				metrics.SetTrustActiveOldestRunAge(*oldestActiveAge)
			}

			var snapshotAge *float64
			if err := pool.QueryRow(ctx, `
				SELECT EXTRACT(EPOCH FROM (now() - MAX(finished_at)))
				FROM trust_runs
				WHERE status = 'succeeded' AND finished_at IS NOT NULL
			`).Scan(&snapshotAge); err != nil {
				log.Error("trust_snapshot_age_query_failed", "error", err)
			} else if snapshotAge != nil {
				metrics.SetTrustActiveSnapshotAge(*snapshotAge)
			}
		}
	}
}

// trustGraphSnapshotRefresher is the slice of *store.PostgresStore the snapshot
// refresh loop needs, declared as an interface for unit testing.
type trustGraphSnapshotRefresher interface {
	RefreshTrustGraphSnapshot(ctx context.Context, maxHops int) (storetrust.TrustGraphSnapshotRefreshResult, error)
}

// globalTrustRunner triggers a global trust run; satisfied by *trust.Runtime.
type globalTrustRunner interface {
	TriggerGlobalRun(ctx context.Context) (trust.Run, error)
}

// runTrustGraphSnapshotRefreshLoop rebuilds trust_graph_snapshot from the
// current seeds + follower_edges on an interval. The snapshot is the BFS
// hop-distance materialization the ingest gate reads to decide trusted authors,
// so it must be refreshed independently of the (heavier) global score runs.
// Runs once immediately so a freshly-started worker populates the snapshot
// without waiting a full interval.
func runTrustGraphSnapshotRefreshLoop(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, refresher trustGraphSnapshotRefresher, maxHops int, every time.Duration) {
	if every <= 0 {
		return
	}
	refresh := func() {
		started := time.Now()
		res, err := refresher.RefreshTrustGraphSnapshot(ctx, maxHops)
		if err != nil {
			log.Error("trust_graph_snapshot_refresh_failed", "error", err)
			return
		}
		log.Info(
			"trust_graph_snapshot_refreshed",
			"rows", res.RowsUpserted,
			"max_hops", maxHops,
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}
	refresh()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

// runTrustRunSchedulerLoop periodically triggers a global trust run (PageRank
// scores). It skips triggering when a run is already pending/running so slow
// compute cannot stack overlapping runs.
func runTrustRunSchedulerLoop(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, pool *pgxpool.Pool, runner globalTrustRunner, every time.Duration) {
	if every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var active int64
			if err := pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM trust_runs
				WHERE status IN ('pending', 'running')
			`).Scan(&active); err != nil {
				log.Error("trust_run_active_check_failed", "error", err)
				continue
			}
			if active > 0 {
				log.Info("trust_run_schedule_skipped", "reason", "run already active", "active", active)
				continue
			}
			run, err := runner.TriggerGlobalRun(ctx)
			if err != nil {
				log.Error("trust_run_schedule_failed", "error", err)
				continue
			}
			log.Info("trust_run_scheduled", "run_id", run.ID)
		}
	}
}

// trustSeedStore is the slice of *store.PostgresStore the seed reconcile needs.
// Declared as an interface so the reconcile logic can be unit-tested with a fake.
type trustSeedStore interface {
	UpsertActiveSeeds(ctx context.Context, pubkeys []string) (int64, error)
	DeactivateMissingSeeds(ctx context.Context, keep []string) (int64, error)
}

// reconcileTrustSeeds makes TRUST_SEED_PUBKEYS authoritative for active seeds:
// configured pubkeys are upserted active and any active seed not in the
// configured set is deactivated.
//
// When no seeds are configured, reconciliation is skipped entirely (rather than
// deactivating everything) so operators can manage trust_seeds manually via SQL.
// When admin seed-management endpoints are added later, this should become
// additive instead of authoritative.
func reconcileTrustSeeds(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, seedStore trustSeedStore, seeds []string) error {
	if len(seeds) == 0 {
		log.Info(
			"trust_seed_reconcile_skipped",
			"reason", "TRUST_SEED_PUBKEYS empty; leaving trust_seeds unmanaged for manual seeding",
		)
		return nil
	}
	activated, err := seedStore.UpsertActiveSeeds(ctx, seeds)
	if err != nil {
		return fmt.Errorf("upsert active seeds: %w", err)
	}
	deactivated, err := seedStore.DeactivateMissingSeeds(ctx, seeds)
	if err != nil {
		return fmt.Errorf("deactivate missing seeds: %w", err)
	}
	log.Info(
		"trust_seed_reconcile_completed",
		"configured", len(seeds),
		"activated", activated,
		"deactivated", deactivated,
	)
	return nil
}

func resolveWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	host = strings.TrimSpace(host)
	if host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}

func trustWorkerModeLabel(enableRedisSync, enableScoreCompute bool) string {
	switch {
	case enableRedisSync && enableScoreCompute:
		return "redis-sync+compute"
	case enableRedisSync:
		return "redis-sync-only"
	case enableScoreCompute:
		return "postgres-only-compute"
	default:
		return "invalid"
	}
}

func resolveBuildVersion(buildVersion, appVersion string) string {
	if v := strings.TrimSpace(buildVersion); v != "" {
		return v
	}
	return strings.TrimSpace(appVersion)
}
