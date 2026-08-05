package runtime

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/relaycontrol"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/runtimebootstrap"
)

func Run(ctx context.Context, log Logger, cfg config.WorkerConfig, build BuildInfo, claimLoop ClaimLoopFn) error {
	bootstrap, shutdown, err := BootstrapRuntime(ctx, log, cfg, build)
	if err != nil {
		return err
	}
	defer shutdown()
	RunLifecycle(ctx, log, cfg, bootstrap, claimLoop)
	return nil
}

func RunLifecycle(ctx context.Context, log Logger, cfg config.WorkerConfig, bootstrap Bootstrap, claimLoop ClaimLoopFn) {
	runtimebootstrap.StartMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	runtimebootstrap.StartDebugEndpoint(ctx, log, cfg.Shared.Observability.DebugAddr)
	logPoolCapacityBudget(log, cfg, bootstrap.Pool)

	// All background loops (retention, sweepers, metrics, claim loops) share a
	// single cancellable context and WaitGroup so shutdown cancels every loop
	// and waits for them to exit before the caller closes the DB pool.
	loopCtx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()
	var wg sync.WaitGroup
	spawn := func(fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(loopCtx)
		}()
	}

	spawn(func(c context.Context) { RunJobRetentionLoop(c, log, bootstrap.Queue, cfg.JobRetention) })
	spawn(func(c context.Context) {
		RunInvalidEventsRetentionLoop(c, log, bootstrap.InvalidEventsStore, cfg.InvalidEventRetention)
	})
	spawn(func(c context.Context) {
		jobs.RunEngagementRetentionLoop(c, log, bootstrap.EngagementStore, jobs.EngagementRetentionConfig{
			Enabled:          cfg.EngagementRetention.Enabled,
			MaxAge:           cfg.EngagementRetention.MaxAge,
			DeadGrace:        cfg.EngagementRetention.DeadGrace,
			RunInterval:      cfg.EngagementRetention.RunInterval,
			DeleteBatchLimit: cfg.EngagementRetention.DeleteBatchLimit,
		})
	})
	spawn(func(c context.Context) {
		jobs.RunReplaceableRetentionLoop(c, log, bootstrap.ReplaceableStore, jobs.ReplaceableRetentionConfig{
			Enabled:          cfg.ReplaceableRetention.Enabled,
			MinAge:           cfg.ReplaceableRetention.MinAge,
			DeadGrace:        cfg.ReplaceableRetention.DeadGrace,
			RunInterval:      cfg.ReplaceableRetention.RunInterval,
			DeleteBatchLimit: cfg.ReplaceableRetention.DeleteBatchLimit,
		})
	})
	spawn(func(c context.Context) {
		jobs.RunDeletionRetentionLoop(c, log, bootstrap.DeletionStore, jobs.DeletionRetentionConfig{
			Enabled:          cfg.DeletionRetention.Enabled,
			MaxAge:           cfg.DeletionRetention.MaxAge,
			DeadGrace:        cfg.DeletionRetention.DeadGrace,
			RunInterval:      cfg.DeletionRetention.RunInterval,
			DeleteBatchLimit: cfg.DeletionRetention.DeleteBatchLimit,
		})
	})
	spawn(func(c context.Context) {
		jobs.RunUntrustedAuthorRetentionLoop(c, log, bootstrap.UntrustedStore, jobs.UntrustedAuthorRetentionConfig{
			Enabled:          cfg.UntrustedAuthorRetention.Enabled,
			MaxAge:           cfg.UntrustedAuthorRetention.MaxAge,
			DeadGrace:        cfg.UntrustedAuthorRetention.DeadGrace,
			RunInterval:      cfg.UntrustedAuthorRetention.RunInterval,
			DeleteBatchLimit: cfg.UntrustedAuthorRetention.DeleteBatchLimit,
		})
	})
	spawn(func(c context.Context) {
		jobs.RunAuthorRecentRetentionLoop(c, log, bootstrap.AuthorRecentStore, jobs.AuthorRecentRetentionConfig{
			Enabled:          cfg.AuthorRecentRetention.Enabled,
			MaxAge:           cfg.AuthorRecentRetention.MaxAge,
			PerAuthorCap:     cfg.AuthorRecentRetention.PerAuthorCap,
			AuthorBatchLimit: cfg.AuthorRecentRetention.AuthorBatchLimit,
			RunInterval:      cfg.AuthorRecentRetention.RunInterval,
			DeleteBatchLimit: cfg.AuthorRecentRetention.DeleteBatchLimit,
		})
	})
	spawn(func(c context.Context) {
		jobs.RunSearchDocsRetentionLoop(c, log, bootstrap.SearchDocsStore, jobs.SearchDocsRetentionConfig{
			Enabled:      cfg.SearchDocsRetention.Enabled,
			BodyMaxAge:   cfg.SearchDocsRetention.BodyMaxAge,
			BodyMaxChars: cfg.SearchDocsRetention.BodyMaxChars,
			RunInterval:  cfg.SearchDocsRetention.RunInterval,
			BatchLimit:   cfg.SearchDocsRetention.BatchLimit,
		})
	})
	spawn(func(c context.Context) {
		jobs.RunEventRelaysRetentionLoop(c, log, bootstrap.EventRelaysStore, jobs.EventRelaysRetentionConfig{
			Enabled:          cfg.EventRelaysRetention.Enabled,
			MaxAge:           cfg.EventRelaysRetention.MaxAge,
			RunInterval:      cfg.EventRelaysRetention.RunInterval,
			DeleteBatchLimit: cfg.EventRelaysRetention.DeleteBatchLimit,
		})
	})
	spawn(func(c context.Context) {
		jobs.RunEventTagsRetentionLoop(c, log, bootstrap.EventTagsStore, jobs.EventTagsRetentionConfig{
			Enabled:          cfg.EventTagsRetention.Enabled,
			RunInterval:      cfg.EventTagsRetention.RunInterval,
			DeleteBatchLimit: cfg.EventTagsRetention.DeleteBatchLimit,
		})
	})
	spawn(func(c context.Context) {
		jobs.RunAppliedStatDeltasRetentionLoop(c, log, bootstrap.AppliedStatDeltasStore, jobs.AppliedStatDeltasRetentionConfig{
			Enabled:          cfg.AppliedStatDeltasRetention.Enabled,
			GracePeriod:      cfg.AppliedStatDeltasRetention.GracePeriod,
			RunInterval:      cfg.AppliedStatDeltasRetention.RunInterval,
			DeleteBatchLimit: cfg.AppliedStatDeltasRetention.DeleteBatchLimit,
		})
	})
	spawn(func(c context.Context) {
		jobs.RunTrustRetentionHooksLoop(c, log, bootstrap.TrustRetention, jobs.TrustRetentionHooksLoopConfig{
			DiscoveryCandidates: jobs.TrustRetentionHookScope{
				Enabled:          cfg.TrustRetentionHooks.DiscoveryProjectionCandidates.Enabled,
				TrustedHorizon:   cfg.TrustRetentionHooks.DiscoveryProjectionCandidates.TrustedHorizon,
				UntrustedHorizon: cfg.TrustRetentionHooks.DiscoveryProjectionCandidates.UntrustedHorizon,
			},
			EnrichmentState: jobs.TrustRetentionHookScope{
				Enabled:          cfg.TrustRetentionHooks.LowValueEnrichmentState.Enabled,
				TrustedHorizon:   cfg.TrustRetentionHooks.LowValueEnrichmentState.TrustedHorizon,
				UntrustedHorizon: cfg.TrustRetentionHooks.LowValueEnrichmentState.UntrustedHorizon,
			},
			RunInterval:      cfg.TrustRetentionLoop.RunInterval,
			DeleteBatchLimit: cfg.TrustRetentionLoop.DeleteBatchLimit,
		})
	})
	spawn(func(c context.Context) { jobs.RunRowCountMetricsReporter(c, log, bootstrap.Pool, 60*time.Second) })
	spawn(func(c context.Context) { RunStorageGovernorLoop(c, log, bootstrap.Store, bootstrap.Queue, cfg) })
	spawn(func(c context.Context) { RunAccountStateRecomputeLoop(c, log, bootstrap.Store, cfg.AccountState) })
	spawn(func(c context.Context) { RunMeilisearchStartupSync(c, log, bootstrap.MeiliClient, bootstrap.Pool) })
	spawn(func(c context.Context) { RunRelayWindowSnapshotsLoop(c, log, bootstrap.Handlers) })
	spawn(func(c context.Context) {
		RunIncrementalStatsReconciliationLoop(c, log, bootstrap.Handlers, cfg.IncrementalStats.Reconciliation)
	})

	if cfg.AuthorAnalyticsSweeper.Enabled {
		// The window list (WORKER_AUTHOR_ANALYTICS_WINDOWS_DAYS) was applied to
		// the derivation handlers at construction; every sweeper goroutine reads
		// the same per-instance list. Invalid/empty values fall back to the
		// package default ([7, 30]).
		for i := 0; i < cfg.AuthorAnalyticsSweeper.Concurrency; i++ {
			workerIdx := i
			spawn(func(c context.Context) {
				RunAuthorAnalyticsSweeperLoop(
					c,
					log,
					bootstrap.Handlers,
					cfg.AuthorAnalyticsSweeper,
					workerIdx,
				)
			})
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
			spawn(func(c context.Context) {
				RunProfileStatsSweeperLoop(
					c,
					log,
					bootstrap.Handlers,
					cfg.ProfileStatsSweeper,
					workerIdx,
				)
			})
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
			spawn(func(c context.Context) {
				RunMeilisearchSweeperLoop(
					c,
					log,
					bootstrap.Handlers,
					cfg.MeilisearchSweeper,
					workerIdx,
				)
			})
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
		spawn(registryController.RunRefreshLoop)
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
		poolName := spec.name
		spawn(func(c context.Context) { RunStaleRecoveryLoop(c, log, bootstrap.Queue, poolName, cfg.JobRecovery) })
	}
	spawn(func(c context.Context) {
		RunQueueAndRebuildMetricsReporter(c, log, bootstrap.Pool, enabledPools, 30*time.Second)
	})

	log.Info(
		"worker_started",
		"worker_id", bootstrap.WorkerID,
		"claim_batch_size", cfg.ClaimBatchSize,
		"default_concurrency", cfg.Concurrency,
		"live_concurrency", cfg.LiveConcurrency,
		"backfill_concurrency", cfg.BackfillConcurrency,
	)

	for _, spec := range specs {
		if spec.concurrency <= 0 {
			continue
		}
		spec := spec
		spawn(func(c context.Context) {
			claimLoop(
				c,
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
		})
	}

	<-ctx.Done()
	cancelLoops()
	waitWithTimeout(&wg, workerDrainTimeout, func() {
		log.Error("worker_background_loops_drain_timeout", "drain_timeout", workerDrainTimeout.String())
	})
	log.Info("shutdown_complete")
}

// workerDrainTimeout bounds how long shutdown waits for background worker loops
// to exit after their context is cancelled, before the caller closes the pool.
const workerDrainTimeout = 30 * time.Second

// waitWithTimeout blocks until wg completes or timeout elapses, invoking onTimeout
// once if the deadline is hit first.
func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration, onTimeout func()) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		if onTimeout != nil {
			onTimeout()
		}
	}
}
