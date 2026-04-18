package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
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
	Handlers           *derivation.Handlers
	WorkerID           string
	MeiliClient        *meili.Client
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
	pool, err := store.OpenPool(ctx, cfg.Shared.Database.URL, cfg.Shared.Database.MaxConns)
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
	if err := derivation.EnsureRegisteredDerivations(ctx, pool); err != nil {
		log.Error("ensure_registered_derivations", "error", err)
		runtimebootstrap.ShutdownTracing(log)
		pool.Close()
		return Bootstrap{}, func() {}, fmt.Errorf("ensure registered derivations: %w", err)
	}
	log.Info("registered_derivations_ready", "count", len(derivation.RegisteredDerivations))

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
		Handlers:    handlers,
		WorkerID:    ResolveWorkerID(),
		MeiliClient: meiliClient,
	}
	shutdown := func() {
		runtimebootstrap.ShutdownTracing(log)
		pool.Close()
	}
	return bootstrap, shutdown, nil
}

// logPoolCapacityBudget reports — and warns when violated — the worker
// process's expected DB-connection demand vs. the configured pgxpool
// MaxConns ceiling. It is a startup diagnostic that surfaced from a
// production incident in which the default pool size of 4 was
// monopolized by sweeper goroutines holding heavy multi-second
// aggregate queries, blocking bundle workers indefinitely at
// pgxpool.Acquire() and producing the symptom of a stalled pipeline
// even though no individual SQL statement was failing.
//
// The demand is the sum of the bundle worker concurrencies and the
// background sweeper concurrencies, plus a small safety reserve for
// ad-hoc queries (metrics reporters, retention loops, registry sync,
// admin endpoints). When demand exceeds capacity by more than the
// reserve, we emit a warning so the operator can either raise
// DATABASE_MAX_CONNS or lower the sweeper concurrencies.
func logPoolCapacityBudget(log Logger, cfg config.WorkerConfig, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	bundleDemand := cfg.Concurrency + cfg.LiveConcurrency + cfg.BackfillConcurrency
	sweeperDemand := 0
	if cfg.AuthorAnalyticsSweeper.Enabled {
		sweeperDemand += cfg.AuthorAnalyticsSweeper.Concurrency
	}
	if cfg.ProfileStatsSweeper.Enabled {
		sweeperDemand += cfg.ProfileStatsSweeper.Concurrency
	}
	if cfg.MeilisearchSweeper.Enabled {
		sweeperDemand += cfg.MeilisearchSweeper.Concurrency
	}
	const ancillaryReserve = 4
	totalDemand := bundleDemand + sweeperDemand + ancillaryReserve
	maxConns := int(pool.Config().MaxConns)
	log.Info(
		"db_pool_capacity_budget",
		"max_conns", maxConns,
		"bundle_demand", bundleDemand,
		"sweeper_demand", sweeperDemand,
		"ancillary_reserve", ancillaryReserve,
		"total_demand", totalDemand,
	)
	if totalDemand > maxConns {
		log.Error(
			"db_pool_capacity_undersized",
			"max_conns", maxConns,
			"total_demand", totalDemand,
			"hint", "raise DATABASE_MAX_CONNS or lower WORKER_AUTHOR_ANALYTICS_SWEEPER_CONCURRENCY / WORKER_PROFILE_STATS_SWEEPER_CONCURRENCY / WORKER_MEILISEARCH_SWEEPER_CONCURRENCY; sweepers run multi-second aggregate queries that monopolize connections and block bundle workers when the pool is undersized",
		)
	}
}

func RunLifecycle(ctx context.Context, log Logger, cfg config.WorkerConfig, bootstrap Bootstrap, claimLoop ClaimLoopFn) {
	runtimebootstrap.StartMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	runtimebootstrap.StartDebugEndpoint(ctx, log, cfg.Shared.Observability.DebugAddr)
	logPoolCapacityBudget(log, cfg, bootstrap.Pool)
	go RunJobRetentionLoop(ctx, log, bootstrap.Queue, cfg.JobRetention)
	go RunInvalidEventsRetentionLoop(ctx, log, bootstrap.InvalidEventsStore, cfg.InvalidEventRetention)
	go jobs.RunRowCountMetricsReporter(ctx, log, bootstrap.Pool, 60*time.Second)
	go RunMeilisearchStartupSync(ctx, log, bootstrap.MeiliClient, bootstrap.Pool)
	go RunRelayWindowSnapshotsLoop(ctx, log, bootstrap.Handlers)

	if cfg.AuthorAnalyticsSweeper.Enabled {
		// Apply WORKER_AUTHOR_ANALYTICS_WINDOWS_DAYS before any sweeper
		// goroutine starts so they all see a consistent window list.
		// SetAuthorAnalyticsWindows silently ignores values outside the
		// schema CHECK ({7, 30, 90}); on an entirely-invalid list it
		// retains the package default ([7, 30]).
		derivation.SetAuthorAnalyticsWindows(cfg.AuthorAnalyticsSweeper.WindowsDays)
		for i := 0; i < cfg.AuthorAnalyticsSweeper.Concurrency; i++ {
			workerIdx := i
			go RunAuthorAnalyticsSweeperLoop(
				ctx,
				log,
				bootstrap.Handlers,
				cfg.AuthorAnalyticsSweeper,
				workerIdx,
			)
		}
		log.Info(
			"author_analytics_sweeper_enabled",
			"concurrency", cfg.AuthorAnalyticsSweeper.Concurrency,
			"interval", cfg.AuthorAnalyticsSweeper.Interval.String(),
			"batch_size", cfg.AuthorAnalyticsSweeper.BatchSize,
			"windows_days", cfg.AuthorAnalyticsSweeper.WindowsDays,
			"rebuild_timeout", cfg.AuthorAnalyticsSweeper.RebuildTimeout.String(),
		)
	} else {
		log.Info("author_analytics_sweeper_disabled")
	}

	if cfg.ProfileStatsSweeper.Enabled {
		for i := 0; i < cfg.ProfileStatsSweeper.Concurrency; i++ {
			workerIdx := i
			go RunProfileStatsSweeperLoop(
				ctx,
				log,
				bootstrap.Handlers,
				cfg.ProfileStatsSweeper,
				workerIdx,
			)
		}
		log.Info(
			"profile_stats_sweeper_enabled",
			"concurrency", cfg.ProfileStatsSweeper.Concurrency,
			"interval", cfg.ProfileStatsSweeper.Interval.String(),
			"batch_size", cfg.ProfileStatsSweeper.BatchSize,
		)
	} else {
		log.Info("profile_stats_sweeper_disabled")
	}

	if cfg.MeilisearchSweeper.Enabled && bootstrap.MeiliClient != nil && bootstrap.MeiliClient.Enabled() {
		for i := 0; i < cfg.MeilisearchSweeper.Concurrency; i++ {
			workerIdx := i
			go RunMeilisearchSweeperLoop(
				ctx,
				log,
				bootstrap.Handlers,
				cfg.MeilisearchSweeper,
				workerIdx,
			)
		}
		log.Info(
			"meilisearch_sweeper_enabled",
			"concurrency", cfg.MeilisearchSweeper.Concurrency,
			"interval", cfg.MeilisearchSweeper.Interval.String(),
			"batch_size", cfg.MeilisearchSweeper.BatchSize,
		)
	} else {
		log.Info("meilisearch_sweeper_disabled")
	}

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
		pollInterval = 1 * time.Second
		retryDelay   = 5 * time.Second
	)

	type poolSpec struct {
		name        string
		concurrency int
	}
	specs := []poolSpec{
		{name: jobs.WorkerPoolDefault, concurrency: cfg.Concurrency},
		{name: jobs.WorkerPoolLive, concurrency: cfg.LiveConcurrency},
		{name: jobs.WorkerPoolBackfill, concurrency: cfg.BackfillConcurrency},
	}

	enabledPools := make([]string, 0, len(specs))
	for _, spec := range specs {
		if spec.concurrency <= 0 {
			log.Info("worker_pool_disabled", "worker_pool", spec.name)
			continue
		}
		enabledPools = append(enabledPools, spec.name)
		go RunStaleRecoveryLoop(ctx, log, bootstrap.Queue, spec.name, cfg.JobRecovery)
	}
	go RunQueueAndRebuildMetricsReporter(ctx, log, bootstrap.Pool, enabledPools, 30*time.Second)

	log.Info(
		"worker_started",
		"worker_id", bootstrap.WorkerID,
		"claim_batch_size", cfg.ClaimBatchSize,
		"default_concurrency", cfg.Concurrency,
		"live_concurrency", cfg.LiveConcurrency,
		"backfill_concurrency", cfg.BackfillConcurrency,
	)

	loopCtx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()

	var wg sync.WaitGroup
	for _, spec := range specs {
		if spec.concurrency <= 0 {
			continue
		}
		spec := spec
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimLoop(
				loopCtx,
				log,
				bootstrap.Queue,
				bootstrap.WorkerID,
				spec.name,
				cfg.ClaimBatchSize,
				spec.concurrency,
				pollInterval,
				retryDelay,
				bootstrap.ProcessJob,
			)
		}()
	}

	<-ctx.Done()
	cancelLoops()
	wg.Wait()
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

// RunJobRetentionLoop is a thin wrapper that delegates to
// jobs.RunRetentionLoop. Kept here so existing callers/imports do not break;
// new callers should depend on jobs.RunRetentionLoop directly to avoid pulling
// the entire worker runtime dependency graph.
func RunJobRetentionLoop(ctx context.Context, log Logger, queue Queue, cfg config.WorkerJobRetentionConfig) {
	jobs.RunRetentionLoop(ctx, log, queue, jobs.RetentionConfig{
		Enabled:          cfg.Enabled,
		SucceededMaxAge:  cfg.SucceededMaxAge,
		DeadMaxAge:       cfg.DeadMaxAge,
		RunInterval:      cfg.RunInterval,
		DeleteBatchLimit: cfg.DeleteBatchLimit,
	})
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
