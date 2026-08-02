package runtime

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/meili"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/runtimebootstrap"
	"github.com/xdzczk/nostrmash/internal/store"
)

type BuildInfo struct {
	Version string
	Commit  string
	Time    string
}

type Bootstrap struct {
	Pool               *pgxpool.Pool
	Queue              Queue
	Store              *store.PostgresStore
	InvalidEventsStore InvalidEventRetentionStore
	EngagementStore    EngagementRetentionStore
	ReplaceableStore   ReplaceableRetentionStore
	DeletionStore      DeletionRetentionStore
	UntrustedStore     UntrustedRetentionStore
	AuthorRecentStore  AuthorRecentRetentionStore
	SearchDocsStore    SearchDocsRetentionStore
	EventRelaysStore   EventRelaysRetentionStore
	TrustRetention     TrustRetentionStore
	ProcessJob         ProcessJobFn
	Handlers           *derivation.Handlers
	WorkerID           string
	MeiliClient        *meili.Client
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
	incProfile := cfg.IncrementalStats.ProfilePublicStats
	incActivity := cfg.IncrementalStats.AuthorActivityDaily
	incRollups := cfg.IncrementalStats.WindowedRollups
	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		MeiliClient: meiliClient,
		// The author-analytics sweeper (spawned below when enabled) reads its
		// window list from the handlers instance; invalid/empty values fall back
		// to the derivation package default.
		AuthorAnalyticsWindows:         cfg.AuthorAnalyticsSweeper.WindowsDays,
		IncrementalProfilePublicStats:  &incProfile,
		IncrementalAuthorActivityDaily: &incActivity,
		IncrementalWindowedRollups:     &incRollups,
	})
	// Retention purges that hard-delete events (expired engagement,
	// untrusted-author) must reverse whatever incremental author-stat
	// deltas those events previously contributed, or profile_public_stats /
	// author_activity_daily / ... drift upward forever as history ages out.
	// Wired here (rather than at store.NewPostgresStore) because Handlers
	// doesn't exist yet at that point.
	postgresStore.Retention.SetIncrementalStatsReverser(handlers)

	hydrationService, err := buildHydrationService(log, cfg, pool, postgresStore)
	if err != nil {
		log.Error("hydration_service_init", "error", err)
		runtimebootstrap.ShutdownTracing(log)
		pool.Close()
		return Bootstrap{}, func() {}, fmt.Errorf("init hydration service: %w", err)
	}

	bootstrap := Bootstrap{
		Pool:               pool,
		Queue:              queue,
		Store:              postgresStore,
		InvalidEventsStore: postgresStore,
		EngagementStore:    postgresStore,
		ReplaceableStore:   postgresStore,
		DeletionStore:      postgresStore,
		UntrustedStore:     postgresStore,
		AuthorRecentStore:  postgresStore,
		SearchDocsStore:    postgresStore,
		EventRelaysStore:   postgresStore,
		TrustRetention:     postgresStore,
		ProcessJob: func(jobCtx context.Context, job jobs.Job) error {
			if job.JobType == jobs.JobTypeHydrateAccount {
				return processHydrateAccountJob(jobCtx, hydrationService, job)
			}
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
