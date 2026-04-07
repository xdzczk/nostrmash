package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/failure"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
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

	appVersion := os.Getenv("APP_VERSION")
	if appVersion == "" {
		appVersion = "dev"
	}
	version := resolveBuildVersion(appVersion)
	if err := traceutil.Init(ctx, cfg.Shared.ServiceName, "trust_worker", version, cfg.Shared.Environment); err != nil {
		log.Error("tracing_init", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := traceutil.Shutdown(shutdownCtx); err != nil {
			log.Error("tracing_shutdown", "error", err)
		}
	}()
	metrics.RegisterBuildInfo("trust_worker", version, strings.TrimSpace(buildCommit), strings.TrimSpace(buildTime))
	metrics.RegisterDeploymentInfo("trust_worker", cfg.Shared.ServiceName, cfg.Shared.Environment)

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

	runMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	runDebugEndpoint(ctx, log, cfg.Shared.Observability.DebugAddr)
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

type workerQueue interface {
	ClaimAvailableForPool(ctx context.Context, workerID, workerPool string, limit int) ([]jobs.Job, error)
	CompleteJob(ctx context.Context, jobID int64, workerID string) error
	FailJob(ctx context.Context, jobID int64, workerID string, errMsg string, retryDelay time.Duration) (jobs.FailureResult, error)
	RecoverStaleRunningJobs(ctx context.Context, olderThan time.Time, limit int) (jobs.RecoveryResult, error)
}

func runClaimLoop(
	ctx context.Context,
	log interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	},
	queue workerQueue,
	workerID string,
	workerPool string,
	batchSize int,
	concurrency int,
	pollInterval time.Duration,
	retryDelay time.Duration,
	processJob func(jobCtx context.Context, job jobs.Job) error,
) {
	if concurrency <= 0 {
		concurrency = 1
	}
	jobQueue := make(chan jobs.Job, batchSize*concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobQueue {
				workerCtx := context.WithoutCancel(ctx)
				started := time.Now()
				err := runJobWithRecovery(workerCtx, job, processJob)
				if err == nil {
					if completeErr := queue.CompleteJob(workerCtx, job.ID, workerID); completeErr != nil {
						log.Error("job_complete_failed", "job_id", job.ID, "error", completeErr)
						continue
					}
					metrics.IncWorkerJob(job.JobType, "succeeded")
					metrics.ObserveWorkerJobExecution(job.JobType, "succeeded", time.Since(started))
					continue
				}
				failState, failErr := queue.FailJob(workerCtx, job.ID, workerID, err.Error(), retryDelay)
				if failErr != nil {
					log.Error("job_fail_mark_failed", "job_id", job.ID, "error", failErr)
					continue
				}
				if failState.Status == jobs.StatusDead {
					metrics.IncWorkerJob(job.JobType, "dead")
					metrics.ObserveWorkerJobExecution(job.JobType, "dead", time.Since(started))
					log.Error("job_dead_lettered", "job_id", job.ID, "job_type", job.JobType, "error", err)
					continue
				}
				metrics.IncWorkerJob(job.JobType, "retry")
				metrics.ObserveWorkerJobExecution(job.JobType, "retry", time.Since(started))
				log.Error("job_failed_retry_scheduled", "job_id", job.ID, "job_type", job.JobType, "error", err)
			}
		}()
	}
	defer func() {
		close(jobQueue)
		wg.Wait()
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		claimed, err := queue.ClaimAvailableForPool(ctx, workerID, workerPool, batchSize)
		if err != nil {
			log.Error("job_claim_failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}
		if len(claimed) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}
		for _, job := range claimed {
			select {
			case <-ctx.Done():
				return
			case jobQueue <- job:
			}
		}
	}
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
			result, err := queue.RecoverStaleRunningJobs(ctx, olderThan, cfg.StaleRecoveryBatchLimit)
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

func runMetricsEndpoint(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Info("metrics_listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics_server", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}

func runDebugEndpoint(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	registerPprofHandlers(mux)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Info("debug_listening", "addr", addr, "surface", "pprof", "auth", "none_bind_private_addr")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("debug_server", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
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

func runJobWithRecovery(ctx context.Context, job jobs.Job, processJob func(jobCtx context.Context, job jobs.Job) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = failure.FromPanic(recovered)
		}
	}()
	return processJob(ctx, job)
}

func registerPprofHandlers(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.Handle("GET /debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("GET /debug/pprof/block", pprof.Handler("block"))
	mux.Handle("GET /debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("GET /debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("GET /debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("GET /debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

func resolveBuildVersion(appVersion string) string {
	if v := strings.TrimSpace(buildVersion); v != "" {
		return v
	}
	return strings.TrimSpace(appVersion)
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
