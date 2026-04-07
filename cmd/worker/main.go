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
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/failure"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

var (
	buildVersion = ""
	buildCommit  = "unknown"
	buildTime    = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("worker")

	cfg, err := config.LoadWorker()
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

	appVersion := os.Getenv("APP_VERSION")
	if appVersion == "" {
		appVersion = "dev"
	}
	version := resolveBuildVersion(appVersion)
	if err := traceutil.Init(ctx, cfg.Shared.ServiceName, "worker", version, cfg.Shared.Environment); err != nil {
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
	metrics.RegisterBuildInfo("worker", version, strings.TrimSpace(buildCommit), strings.TrimSpace(buildTime))
	metrics.RegisterDeploymentInfo("worker", cfg.Shared.ServiceName, cfg.Shared.Environment)
	log.Info("build_info",
		"binary_role", "worker",
		"version", version,
		"commit", strings.TrimSpace(buildCommit),
		"build_time", strings.TrimSpace(buildTime),
		"environment", cfg.Shared.Environment,
	)
	if err := store.Migrate(ctx, pool, appVersion); err != nil {
		log.Error("migrate", "error", err)
		os.Exit(1)
	}

	queue := jobs.NewQueue(pool)
	postgresStore := store.NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	workerID := resolveWorkerID()
	runMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	runDebugEndpoint(ctx, log, cfg.Shared.Observability.DebugAddr)
	go runQueueAndRebuildMetricsReporter(ctx, log, pool, 30*time.Second)
	go runJobRetentionLoop(ctx, log, queue, cfg.JobRetention)
	go runInvalidEventsRetentionLoop(ctx, log, postgresStore, cfg.InvalidEventRetention)
	const (
		claimBatchSize = 10
		pollInterval   = 1 * time.Second
		retryDelay     = 5 * time.Second
	)

	log.Info("worker_started", "worker_id", workerID, "claim_batch_size", claimBatchSize)
	runClaimLoop(
		ctx,
		log,
		queue,
		workerID,
		jobs.WorkerPoolDefault,
		claimBatchSize,
		cfg.Concurrency,
		pollInterval,
		retryDelay,
		func(jobCtx context.Context, job jobs.Job) error {
			return derivation.ProcessJob(jobCtx, handlers, derivation.Job{
				JobType: job.JobType,
				Payload: job.Payload,
			})
		},
	)

	<-ctx.Done()
	log.Info("shutdown_complete")
}

type workerQueue interface {
	ClaimAvailableForPool(ctx context.Context, workerID, workerPool string, limit int) ([]jobs.Job, error)
	CompleteJob(ctx context.Context, jobID int64, workerID string) error
	FailJob(ctx context.Context, jobID int64, workerID string, errMsg string, retryDelay time.Duration) (jobs.FailureResult, error)
	PurgeTerminalJobs(ctx context.Context, succeededBefore, deadBefore time.Time, limit int) (int64, error)
}

type invalidEventRetentionStore interface {
	PurgeInvalidEventsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	TrimInvalidEventPayloadsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

func runJobRetentionLoop(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, queue workerQueue, cfg config.WorkerJobRetentionConfig) {
	if !cfg.Enabled {
		log.Info("job_retention_disabled")
		return
	}
	if cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error("job_retention_invalid_config", "run_interval", cfg.RunInterval.String(), "delete_batch_limit", cfg.DeleteBatchLimit)
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
			now := time.Now().UTC()
			succeededBefore := now.Add(-cfg.SucceededMaxAge)
			deadBefore := now.Add(-cfg.DeadMaxAge)
			deleted, err := queue.PurgeTerminalJobs(ctx, succeededBefore, deadBefore, cfg.DeleteBatchLimit)
			if err != nil {
				metrics.IncRetentionPurgeRun("jobs_terminal", "error")
				log.Error("job_retention_purge_failed", "error", err)
				continue
			}
			metrics.IncRetentionPurgeRun("jobs_terminal", "ok")
			metrics.AddRetentionPurgedRows("jobs_terminal", deleted)
			if deleted > 0 {
				log.Info("job_retention_purged", "deleted", deleted, "succeeded_before", succeededBefore.Format(time.RFC3339), "dead_before", deadBefore.Format(time.RFC3339))
			}
		}
	}
}

func runInvalidEventsRetentionLoop(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, store invalidEventRetentionStore, cfg config.WorkerInvalidEventRetentionConfig) {
	if !cfg.Enabled {
		log.Info("invalid_events_retention_disabled")
		return
	}
	if cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error("invalid_events_retention_invalid_config", "run_interval", cfg.RunInterval.String(), "delete_batch_limit", cfg.DeleteBatchLimit)
		return
	}
	log.Info(
		"invalid_events_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
		"payload_trim_enabled", cfg.PayloadTrim.Enabled,
		"payload_trim_max_age", cfg.PayloadTrim.MaxAge.String(),
		"payload_trim_batch_limit", cfg.PayloadTrim.BatchLimit,
	)

	ticker := time.NewTicker(cfg.RunInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().UTC().Add(-cfg.MaxAge)
			deleted, err := store.PurgeInvalidEventsOlderThan(ctx, cutoff, cfg.DeleteBatchLimit)
			if err != nil {
				metrics.IncRetentionPurgeRun("invalid_events", "error")
				log.Error("invalid_events_retention_purge_failed", "error", err)
				continue
			}
			metrics.IncRetentionPurgeRun("invalid_events", "ok")
			metrics.AddRetentionPurgedRows("invalid_events", deleted)
			if deleted > 0 {
				log.Info("invalid_events_retention_purged", "deleted", deleted, "cutoff", cutoff.Format(time.RFC3339))
			}
			if !cfg.PayloadTrim.Enabled {
				continue
			}

			trimCutoff := time.Now().UTC().Add(-cfg.PayloadTrim.MaxAge)
			trimmed, trimErr := store.TrimInvalidEventPayloadsOlderThan(ctx, trimCutoff, cfg.PayloadTrim.BatchLimit)
			if trimErr != nil {
				metrics.IncRetentionPurgeRun("invalid_events_payload", "error")
				log.Error("invalid_events_payload_trim_failed", "error", trimErr)
				continue
			}
			metrics.IncRetentionPurgeRun("invalid_events_payload", "ok")
			metrics.AddRetentionPurgedRows("invalid_events_payload", trimmed)
			if trimmed > 0 {
				log.Info("invalid_events_payload_trimmed", "trimmed", trimmed, "cutoff", trimCutoff.Format(time.RFC3339))
			}
		}
	}
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
				// Drain in-flight jobs even after shutdown signal.
				workerCtx := context.WithoutCancel(ctx)
				spanCtx, span := traceutil.StartSpan(workerCtx, "worker.job.execute",
					traceutil.KV("job.type", job.JobType),
				)
				started := time.Now()
				err := runJobWithRecovery(spanCtx, job, processJob)
				if err == nil {
					if completeErr := queue.CompleteJob(spanCtx, job.ID, workerID); completeErr != nil {
						class := failure.ClassifyError(completeErr)
						metrics.ObserveWorkerJobExecution(job.JobType, "complete_error", time.Since(started))
						span.End(completeErr)
						log.Error("job_complete_failed", "job_id", job.ID, "failure_class", class.Class, "failure_reason", class.Reason, "error", completeErr)
						continue
					}
					metrics.IncWorkerJob(job.JobType, "succeeded")
					metrics.ObserveWorkerJobExecution(job.JobType, "succeeded", time.Since(started))
					span.End(nil)
					log.Info("job_completed", "job_id", job.ID, "job_type", job.JobType)
					continue
				}

				failState, failErr := queue.FailJob(spanCtx, job.ID, workerID, err.Error(), retryDelay)
				if failErr != nil {
					class := failure.ClassifyError(failErr)
					metrics.ObserveWorkerJobExecution(job.JobType, "fail_mark_error", time.Since(started))
					span.End(failErr)
					log.Error("job_fail_mark_failed", "job_id", job.ID, "failure_class", class.Class, "failure_reason", class.Reason, "error", failErr)
					continue
				}
				errClass := failure.ClassifyError(err)
				if failState.Status == jobs.StatusDead {
					metrics.IncWorkerJob(job.JobType, "dead")
					metrics.ObserveWorkerJobExecution(job.JobType, "dead", time.Since(started))
					span.End(err)
					log.Error(
						"job_dead_lettered",
						"job_id", job.ID,
						"job_type", job.JobType,
						"failure_class", errClass.Class,
						"failure_reason", errClass.Reason,
						"attempts", failState.Attempts,
						"max_attempts", failState.MaxAttempts,
						"error", err,
					)
					continue
				}
				metrics.IncWorkerJob(job.JobType, "retry")
				metrics.ObserveWorkerJobExecution(job.JobType, "retry", time.Since(started))
				span.End(err)
				log.Error(
					"job_failed_retry_scheduled",
					"job_id", job.ID,
					"job_type", job.JobType,
					"failure_class", errClass.Class,
					"failure_reason", errClass.Reason,
					"attempts", failState.Attempts,
					"max_attempts", failState.MaxAttempts,
					"retry_after", retryDelay.String(),
					"error", err,
				)
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

		claimCtx, claimSpan := traceutil.StartSpan(ctx, "worker.queue.claim_available")
		claimed, err := queue.ClaimAvailableForPool(claimCtx, workerID, workerPool, batchSize)
		claimSpan.End(err)
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

func runQueueAndRebuildMetricsReporter(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, pool *pgxpool.Pool, every time.Duration) {
	if pool == nil || every <= 0 {
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
				WHERE status = 'pending'
				  AND worker_pool = 'default'
			`).Scan(&oldestPending); err != nil {
				log.Error("queue_backlog_metrics_query_failed", "error", err)
			} else if oldestPending != nil {
				metrics.SetWorkerQueueBacklogOldestPendingAge(*oldestPending)
			} else {
				metrics.SetWorkerQueueBacklogOldestPendingAge(0)
			}

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
