package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/meili"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/relaycontrol"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/runtimebootstrap"
	"github.com/xdzczk/nostrmash/internal/store"
)

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type Queue interface {
	ClaimAvailableForPool(ctx context.Context, workerID, workerPool string, limit int) ([]jobs.Job, error)
	CompleteJob(ctx context.Context, jobID int64, workerID string) error
	FailJob(ctx context.Context, jobID int64, workerID string, errMsg string, retryDelay time.Duration) (jobs.FailureResult, error)
	RecoverStaleRunningJobs(ctx context.Context, workerPool string, olderThan time.Time, limit int) (jobs.RecoveryResult, error)
	PurgeTerminalJobs(ctx context.Context, succeededBefore, deadBefore time.Time, limit int) (int64, error)
}

type InvalidEventRetentionStore interface {
	PurgeInvalidEventsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	TrimInvalidEventPayloadsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

type ProcessJobFn func(jobCtx context.Context, job jobs.Job) error

type ClaimLoopFn func(
	ctx context.Context,
	log Logger,
	queue Queue,
	workerID string,
	workerPool string,
	batchSize int,
	concurrency int,
	pollInterval time.Duration,
	retryDelay time.Duration,
	processJob ProcessJobFn,
)

type BuildInfo struct {
	Version string
	Commit  string
	Time    string
}

type Bootstrap struct {
	Pool               *pgxpool.Pool
	Queue              Queue
	InvalidEventsStore InvalidEventRetentionStore
	ProcessJob         ProcessJobFn
	WorkerID           string
}

func Run(ctx context.Context, log Logger, cfg config.WorkerConfig, build BuildInfo, claimLoop ClaimLoopFn) error {
	bootstrap, shutdown, err := BootstrapRuntime(ctx, log, cfg, build)
	if err != nil {
		return err
	}
	defer shutdown()
	RunLifecycle(ctx, log, cfg, bootstrap, claimLoop)
	return nil
}

func BootstrapRuntime(ctx context.Context, log Logger, cfg config.WorkerConfig, build BuildInfo) (Bootstrap, func(), error) {
	pool, err := store.OpenPool(ctx, cfg.Shared.Database.URL)
	if err != nil {
		log.Error("db_connect", "error", err)
		return Bootstrap{}, func() {}, fmt.Errorf("db connect: %w", err)
	}
	metrics.RegisterDBPool(pool)

	appVersion := runtimebootstrap.ResolveAppVersion()
	version := ResolveBuildVersion(appVersion, build.Version)
	if err := runtimebootstrap.InitTracing(ctx, log, cfg.Shared.ServiceName, "worker", version, cfg.Shared.Environment); err != nil {
		log.Error("tracing_init", "error", err)
		pool.Close()
		return Bootstrap{}, func() {}, fmt.Errorf("tracing init: %w", err)
	}
	runtimebootstrap.RegisterBuildAndDeployment(
		log,
		"worker",
		cfg.Shared.ServiceName,
		cfg.Shared.Environment,
		version,
		build.Commit,
		build.Time,
	)
	if err := store.Migrate(ctx, pool, appVersion); err != nil {
		log.Error("migrate", "error", err)
		runtimebootstrap.ShutdownTracing(log)
		pool.Close()
		return Bootstrap{}, func() {}, fmt.Errorf("migrate: %w", err)
	}

	queue := jobs.NewQueue(pool)
	postgresStore := store.NewPostgresStore(pool)
	meiliClient, err := meili.NewClient(meili.Config{
		Enabled:      cfg.Meilisearch.Enabled,
		URL:          cfg.Meilisearch.URL,
		MasterKey:    cfg.Meilisearch.MasterKey,
		SearchAPIKey: cfg.Meilisearch.SearchAPIKey,
	})
	if err != nil {
		runtimebootstrap.ShutdownTracing(log)
		pool.Close()
		return Bootstrap{}, func() {}, fmt.Errorf("init meilisearch client: %w", err)
	}
	if meiliClient.Enabled() {
		if err := meiliClient.EnsureIndexes(ctx); err != nil {
			runtimebootstrap.ShutdownTracing(log)
			pool.Close()
			return Bootstrap{}, func() {}, fmt.Errorf("ensure meilisearch indexes: %w", err)
		}
		needsSync, syncCheckErr := meiliClient.NeedsSync(ctx, pool)
		if syncCheckErr != nil {
			log.Error("meilisearch_sync_check", "error", syncCheckErr)
		} else if needsSync {
			log.Info("meilisearch_indexes_stale", "action", "starting_full_sync")
			stats, syncErr := meiliClient.FullSync(ctx, pool, 1000)
			if syncErr != nil {
				log.Error("meilisearch_startup_sync_failed", "error", syncErr)
			} else {
				log.Info("meilisearch_startup_sync_complete",
					"profiles", stats.Profiles,
					"notes", stats.Notes,
					"documents", stats.Documents,
				)
			}
		}
	}
	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		MeiliClient: meiliClient,
	})

	bootstrap := Bootstrap{
		Pool:               pool,
		Queue:              queue,
		InvalidEventsStore: postgresStore,
		ProcessJob: func(jobCtx context.Context, job jobs.Job) error {
			return derivation.ProcessJob(jobCtx, handlers, derivation.Job{
				JobType: job.JobType,
				Payload: job.Payload,
			})
		},
		WorkerID: ResolveWorkerID(),
	}
	shutdown := func() {
		runtimebootstrap.ShutdownTracing(log)
		pool.Close()
	}
	return bootstrap, shutdown, nil
}

func RunLifecycle(ctx context.Context, log Logger, cfg config.WorkerConfig, bootstrap Bootstrap, claimLoop ClaimLoopFn) {
	runtimebootstrap.StartMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	runtimebootstrap.StartDebugEndpoint(ctx, log, cfg.Shared.Observability.DebugAddr)
	go RunQueueAndRebuildMetricsReporter(ctx, log, bootstrap.Pool, 30*time.Second)
	go RunStaleRecoveryLoop(ctx, log, bootstrap.Queue, jobs.WorkerPoolDefault, cfg.JobRecovery)
	go RunJobRetentionLoop(ctx, log, bootstrap.Queue, cfg.JobRetention)
	go RunInvalidEventsRetentionLoop(ctx, log, bootstrap.InvalidEventsStore, cfg.InvalidEventRetention)

	if cfg.RelayRegistry.Enabled {
		slogLogger, ok := any(log).(*slog.Logger)
		if !ok {
			slogLogger = slog.Default()
		}
		registryStore := relayregistry.NewStore(bootstrap.Pool)
		registryController := relaycontrol.NewController(slogLogger, registryStore, bootstrap.Pool, cfg.RelayRegistry)
		go registryController.RunRefreshLoop(ctx)
	}
	const (
		claimBatchSize = 10
		pollInterval   = 1 * time.Second
		retryDelay     = 5 * time.Second
	)

	log.Info("worker_started", "worker_id", bootstrap.WorkerID, "claim_batch_size", claimBatchSize)
	claimLoop(
		ctx,
		log,
		bootstrap.Queue,
		bootstrap.WorkerID,
		jobs.WorkerPoolDefault,
		claimBatchSize,
		cfg.Concurrency,
		pollInterval,
		retryDelay,
		bootstrap.ProcessJob,
	)

	<-ctx.Done()
	log.Info("shutdown_complete")
}

func ResolveBuildVersion(appVersion, buildVersion string) string {
	return runtimebootstrap.ResolveBuildVersion(appVersion, buildVersion)
}

func ResolveWorkerID() string {
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

func RunJobRetentionLoop(ctx context.Context, log Logger, queue Queue, cfg config.WorkerJobRetentionConfig) {
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

func RunInvalidEventsRetentionLoop(ctx context.Context, log Logger, store InvalidEventRetentionStore, cfg config.WorkerInvalidEventRetentionConfig) {
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

func RunQueueAndRebuildMetricsReporter(ctx context.Context, log Logger, pool *pgxpool.Pool, every time.Duration) {
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
