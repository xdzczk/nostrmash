package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/runtimebootstrap"
	"github.com/xdzczk/nostrmash/internal/store"
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

	appVersion := runtimebootstrap.ResolveAppVersion()
	version := runtimebootstrap.ResolveBuildVersion(appVersion, buildVersion)
	if err := runtimebootstrap.InitTracing(ctx, log, cfg.Shared.ServiceName, "worker", version, cfg.Shared.Environment); err != nil {
		log.Error("tracing_init", "error", err)
		os.Exit(1)
	}
	defer runtimebootstrap.ShutdownTracing(log)
	runtimebootstrap.RegisterBuildAndDeployment(
		log,
		"worker",
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

	queue := jobs.NewQueue(pool)
	postgresStore := store.NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	workerID := resolveWorkerID()
	runtimebootstrap.StartMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	runtimebootstrap.StartDebugEndpoint(ctx, log, cfg.Shared.Observability.DebugAddr)
	go runQueueAndRebuildMetricsReporter(ctx, log, pool, 30*time.Second)
	go runStaleRecoveryLoop(ctx, log, queue, jobs.WorkerPoolDefault, cfg.JobRecovery)
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
