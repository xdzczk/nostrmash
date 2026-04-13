package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/runtimebootstrap"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/trust"
)

var (
	buildVersion = ""
	buildCommit  = "unknown"
	buildTime    = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("trust_worker")
	slog.SetDefault(log)
	cfg, err := config.LoadTrustWorker()
	if err != nil {
		log.Error("config", "error", err)
		os.Exit(1)
	}

	pool, err := store.OpenPool(ctx, cfg.Shared.Database.URL)
	if err != nil {
		log.Error("db_connect", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	metrics.RegisterDBPool(pool)

	var redisClient *redis.Client
	if cfg.EnableRedisSync {
		redisOpts, err := redis.ParseURL(cfg.Redis.URL)
		if err != nil {
			log.Error("redis_parse_url", "error", err)
			os.Exit(1)
		}
		redisClient = redis.NewClient(redisOpts)
		defer func() { _ = redisClient.Close() }()
		if err := redisClient.Ping(ctx).Err(); err != nil {
			log.Error("redis_ping", "error", err)
			os.Exit(1)
		}
	}

	appVersion := runtimebootstrap.ResolveAppVersion()
	version := runtimebootstrap.ResolveBuildVersion(appVersion, buildVersion)
	if err := runtimebootstrap.InitTracing(ctx, log, cfg.Shared.ServiceName, "trust_worker", version, cfg.Shared.Environment); err != nil {
		log.Error("tracing_init", "error", err)
		os.Exit(1)
	}
	defer runtimebootstrap.ShutdownTracing(log)
	runtimebootstrap.RegisterBuildAndDeployment(
		log,
		"trust_worker",
		cfg.Shared.ServiceName,
		cfg.Shared.Environment,
		version,
		buildCommit,
		buildTime,
	)

	if err := store.Migrate(ctx, pool, appVersion); err != nil {
		log.Error("migrate", "error", err)
		os.Exit(1)
	}

	log.Info(
		"trust_worker_startup_mode",
		"redis_sync_enabled", cfg.EnableRedisSync,
		"score_compute_enabled", cfg.EnableScoreCompute,
		"mode", trustWorkerModeLabel(cfg.EnableRedisSync, cfg.EnableScoreCompute),
	)

	queue := jobs.NewQueue(pool)
	var runtime *trust.Runtime
	if cfg.EnableRedisSync {
		runtime = trust.NewRuntimeWithRedis(
			pool,
			redisClient,
			cfg.Redis.KeyPrefix,
			cfg.EnableRedisSync,
			cfg.EnableScoreCompute,
		)
	} else {
		runtime = trust.NewRuntimeWithRedis(
			pool,
			nil,
			cfg.Redis.KeyPrefix,
			cfg.EnableRedisSync,
			cfg.EnableScoreCompute,
		)
	}
	workerID := resolveWorkerID()

	runtimebootstrap.StartMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	runtimebootstrap.StartDebugEndpoint(ctx, log, cfg.Shared.Observability.DebugAddr)
	go runTrustMetricsReporter(ctx, log, pool, 30*time.Second)
	go runStaleRecoveryLoop(ctx, log, queue, jobs.WorkerPoolTrust, cfg.JobRecovery)

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

func resolveBuildVersion(appVersion string) string {
	return runtimebootstrap.ResolveBuildVersion(appVersion, buildVersion)
}
