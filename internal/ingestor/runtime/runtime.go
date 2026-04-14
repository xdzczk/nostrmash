package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
	"github.com/xdzczk/nostrmash/internal/ingestor/live"
	"github.com/xdzczk/nostrmash/internal/ingestor/relay"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/replay"
	"github.com/xdzczk/nostrmash/internal/runtimebootstrap"
	"github.com/xdzczk/nostrmash/internal/store"
)

type BuildInfo struct {
	Version string
	Commit  string
	Time    string
}

type runner struct {
	eventStore *store.PostgresStore
	processor  *live.Processor
	kinds      []int
}

func Run(ctx context.Context, log *slog.Logger, cfg config.IngestorConfig, build BuildInfo) error {
	pool, shutdown, err := BootstrapRuntime(ctx, log, cfg, build)
	if err != nil {
		return err
	}
	defer shutdown()

	runner, err := buildRunner(log, cfg, pool)
	if err != nil {
		return err
	}
	return runLifecycle(ctx, log, cfg, pool, runner)
}

func BootstrapRuntime(
	ctx context.Context,
	log *slog.Logger,
	cfg config.IngestorConfig,
	build BuildInfo,
) (*pgxpool.Pool, func(), error) {
	pool, err := store.OpenPool(ctx, cfg.Shared.Database.URL)
	if err != nil {
		log.Error("db_connect", "error", err)
		return nil, func() {}, fmt.Errorf("db connect: %w", err)
	}
	metrics.RegisterDBPool(pool)

	appVersion := runtimebootstrap.ResolveAppVersion()
	version := ResolveBuildVersion(appVersion, build.Version)
	if err := runtimebootstrap.InitTracing(ctx, log, cfg.Shared.ServiceName, "ingestor", version, cfg.Shared.Environment); err != nil {
		log.Error("tracing_init", "error", err)
		pool.Close()
		return nil, func() {}, fmt.Errorf("tracing init: %w", err)
	}
	runtimebootstrap.RegisterBuildAndDeployment(
		log,
		"ingestor",
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
		return nil, func() {}, fmt.Errorf("migrate: %w", err)
	}
	shutdown := func() {
		runtimebootstrap.ShutdownTracing(log)
		pool.Close()
	}
	return pool, shutdown, nil
}

func buildRunner(
	log *slog.Logger,
	cfg config.IngestorConfig,
	pool *pgxpool.Pool,
) (runner, error) {
	kinds, err := ResolveLiveKinds(cfg.Relay)
	if err != nil {
		log.Error("ingestor_filter_group", "error", err)
		return runner{}, err
	}

	eventStore := store.NewPostgresStore(pool)
	processor, err := live.NewProcessor(log, eventStore, nostr.Options{})
	if err != nil {
		log.Error("ingestor_processor", "error", err)
		return runner{}, err
	}

	return runner{
		eventStore: eventStore,
		processor:  processor,
		kinds:      kinds,
	}, nil
}

func runLifecycle(
	ctx context.Context,
	log *slog.Logger,
	cfg config.IngestorConfig,
	pool *pgxpool.Pool,
	runner runner,
) error {
	var (
		checkpointTracker *live.CheckpointTracker
		sinceResolver     relay.SinceResolver
		err               error
	)
	runtimebootstrap.StartMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	go runMetricsLogger(ctx, log, runner.processor, 30*time.Second)

	if cfg.Runtime.Mode == "replay" {
		replayRunner, replayErr := replay.NewRunner(log, pool, nostr.Options{})
		if replayErr != nil {
			log.Error("replay_runner_init", "error", replayErr)
			return replayErr
		}
		result, replayErr := replayRunner.ReplayFixturePath(ctx, cfg.Replay.FixturePath)
		if replayErr != nil {
			log.Error("replay_failed", "error", replayErr)
			return replayErr
		}
		log.Info(
			"replay_completed",
			"fixture_path", cfg.Replay.FixturePath,
			"entries_replayed", result.EntriesReplayed,
			"jobs_processed", result.JobsProcessed,
			"valid_total", result.IngestCounters.Valid,
			"duplicate_total", result.IngestCounters.Duplicate,
			"invalid_total", result.IngestCounters.Invalid,
		)
		return nil
	}

	checkpointTracker, err = live.NewCheckpointTracker(
		log,
		runner.eventStore,
		cfg.Relay.ActiveFilterGroup,
		5*time.Second,
	)
	if err != nil {
		log.Error("live_checkpoint_tracker", "error", err)
		return err
	}
	runner.processor.SetCheckpointWriter(checkpointTracker)
	sinceResolver, err = live.NewResumeSinceResolver(
		runner.eventStore,
		cfg.Relay.ActiveFilterGroup,
		cfg.Relay.LiveBootstrapLookbackSeconds,
		cfg.Relay.LiveResumeOverlapSeconds,
	)
	if err != nil {
		log.Error("live_since_resolver", "error", err)
		return err
	}
	log.Info(
		"ingestor_live_startup",
		"mode", cfg.Runtime.Mode,
		"filter_group", cfg.Relay.ActiveFilterGroup,
		"relay_count", len(cfg.Relay.URLs),
		"relay_urls", cfg.Relay.URLs,
		"resume_checkpoint_enabled", true,
		"bootstrap_lookback_seconds", cfg.Relay.LiveBootstrapLookbackSeconds,
		"resume_overlap_seconds", cfg.Relay.LiveResumeOverlapSeconds,
	)
	go runCheckpointFreshnessReporter(ctx, log, pool, cfg.Relay.ActiveFilterGroup, 30*time.Second)

	prioritizedRelays := append([]string(nil), cfg.Relay.URLs...)
	if cfg.TrustPrioritization.Enabled {
		prioritizedRelays, err = prioritizeRelaysByTrust(ctx, pool, cfg.Relay.URLs, cfg.TrustPrioritization.TopPubkeys)
		if err != nil {
			log.Warn("trust_relay_prioritization_failed", "error", err)
			prioritizedRelays = append([]string(nil), cfg.Relay.URLs...)
			metrics.IncIngestRelayPriorityDecision("fallback_error")
		} else {
			metrics.IncIngestRelayPriorityDecision("applied")
		}
	} else {
		metrics.IncIngestRelayPriorityDecision("disabled")
		log.Info("trust_relay_prioritization_disabled")
	}

	if cfg.Backfill.Enabled {
		backfillRunner, backfillErr := backfill.NewRunner(
			log,
			backfill.Config{
				Relays:       prioritizedRelays,
				FilterGroup:  cfg.Relay.ActiveFilterGroup,
				Kinds:        runner.kinds,
				Mode:         cfg.Backfill.Mode,
				Since:        cfg.Backfill.Since,
				Until:        cfg.Backfill.Until,
				PageLimit:    cfg.Backfill.PageLimit,
				EmptyPageMax: cfg.Backfill.EmptyPageMax,
			},
			runner.eventStore,
			backfill.WebsocketFetcher{
				Log:            log,
				ConnectTimeout: cfg.Backfill.ConnectTimeout,
				IdleTimeout:    cfg.Backfill.IdleTimeout,
			},
			runner.processor.Handle,
		)
		if backfillErr != nil {
			log.Error("backfill_runner_config", "error", backfillErr)
			return backfillErr
		}
		if backfillErr := backfillRunner.Run(ctx); backfillErr != nil {
			log.Error("backfill_runner", "error", backfillErr)
			return backfillErr
		}
		log.Info("backfill_completed", "relay_count", len(prioritizedRelays))
	}

	if cfg.TrustFetch.Enabled {
		go runTrustTargetedFetchLoop(
			ctx,
			log,
			runner.eventStore,
			cfg.TrustPrioritization,
			cfg.TrustFetch,
			prioritizedRelays,
			backfill.WebsocketFetcher{
				Log:            log,
				ConnectTimeout: cfg.Backfill.ConnectTimeout,
				IdleTimeout:    cfg.Backfill.IdleTimeout,
			},
			runner.processor.Handle,
		)
		log.Info(
			"trust_fetch_started",
			"max_tracked_pubkeys", cfg.TrustFetch.MaxTrackedPubkeys,
			"max_selected_per_cycle", cfg.TrustFetch.MaxSelectedPerCycle,
			"refresh_interval", cfg.TrustFetch.RefreshInterval.String(),
		)
	}

	if cfg.AuthorMetadataDiscovery.Enabled {
		go runAuthorMetadataDiscoveryLoop(
			ctx,
			log,
			runner.eventStore,
			cfg.AuthorMetadataDiscovery,
			prioritizedRelays,
			backfill.WebsocketFetcher{
				Log:            log,
				ConnectTimeout: cfg.Backfill.ConnectTimeout,
				IdleTimeout:    cfg.Backfill.IdleTimeout,
			},
			runner.processor.Handle,
		)
		log.Info(
			"author_metadata_discovery_started",
			"batch_size", cfg.AuthorMetadataDiscovery.BatchSize,
			"interval", cfg.AuthorMetadataDiscovery.Interval.String(),
		)
	}

	if cfg.RelayRegistry.Enabled {
		registryStore := relayregistry.NewStore(pool)
		reconciler := relay.NewReconciler(
			log,
			relay.NostrConnector{
				Log:           log,
				Kinds:         runner.kinds,
				FilterGroup:   cfg.Relay.ActiveFilterGroup,
				SinceResolver: sinceResolver,
			},
			runner.processor.Handle,
			checkpointTracker,
			registryStore,
			relay.ReconcilerConfig{
				PollInterval: cfg.RelayRegistry.Reconcile.PollInterval,
				DrainTimeout: cfg.RelayRegistry.Reconcile.DrainTimeout,
				FallbackURLs: prioritizedRelays,
				ConnectConfig: relay.Config{
					ConnectTimeout: cfg.Relay.ConnectTimeout,
					InitialBackoff: cfg.Relay.InitialBackoff,
					MaxBackoff:     cfg.Relay.MaxBackoff,
					LagThreshold:   cfg.Relay.LagThreshold,
				},
			},
		)
		go reconciler.Run(ctx)
		log.Info("relay_reconciler_started", "fallback_relay_count", len(prioritizedRelays))
	} else {
		relayManager, err := relay.NewManager(
			relay.Config{
				Relays:         prioritizedRelays,
				Allowlist:      cfg.Relay.Allowlist,
				DisabledRelays: cfg.Relay.Disabled,
				ConnectTimeout: cfg.Relay.ConnectTimeout,
				InitialBackoff: cfg.Relay.InitialBackoff,
				MaxBackoff:     cfg.Relay.MaxBackoff,
				LagThreshold:   cfg.Relay.LagThreshold,
				StatusSink:     checkpointTracker,
			},
			relay.NostrConnector{
				Log:           log,
				Kinds:         runner.kinds,
				FilterGroup:   cfg.Relay.ActiveFilterGroup,
				SinceResolver: sinceResolver,
			},
			runner.processor.Handle,
			log,
		)
		if err != nil {
			log.Error("relay_config", "error", err)
			return err
		}
		go relayManager.Start(ctx)
		log.Info("relay_manager_started", "relay_count", len(prioritizedRelays))
	}

	<-ctx.Done()
	if checkpointTracker != nil {
		for _, relayURL := range cfg.Relay.URLs {
			if setErr := checkpointTracker.SetRelayStatus(context.Background(), relayURL, relay.StateDisconnected, ""); setErr != nil {
				log.Warn("live_checkpoint_disconnect_shutdown", "relay_url", relayURL, "error", setErr)
			}
		}
		if flushErr := checkpointTracker.FlushAll(context.Background()); flushErr != nil {
			log.Warn("live_checkpoint_flush_shutdown", "error", flushErr)
		}
	}
	final := runner.processor.Snapshot()
	log.Info(
		"ingest_metrics_final",
		"valid_total", final.Valid,
		"duplicate_total", final.Duplicate,
		"invalid_total", final.Invalid,
	)
	log.Info("shutdown_complete")
	return nil
}

func prioritizeRelaysByTrust(ctx context.Context, pool *pgxpool.Pool, relays []string, topPubkeys int) ([]string, error) {
	if pool == nil || len(relays) == 0 {
		return append([]string(nil), relays...), nil
	}
	eventStore := store.NewPostgresStore(pool)
	sorted, err := eventStore.PrioritizeConfiguredRelaysByTrust(ctx, relays, topPubkeys)
	if err != nil {
		return nil, fmt.Errorf("prioritize relays by trust: %w", err)
	}
	return sorted, nil
}

func SortRelaysByWeights(normalized []string, baseOrder map[string]int, weights map[string]float64) []string {
	return store.SortRelaysByWeights(normalized, baseOrder, weights)
}

func ResolveLiveKinds(cfg config.RelayConfig) ([]int, error) {
	activeGroup := strings.TrimSpace(cfg.ActiveFilterGroup)
	group, ok := cfg.FilterGroups[activeGroup]
	if !ok {
		return nil, fmt.Errorf("active filter group %q is not defined", activeGroup)
	}
	if activeGroup != "default_v1" {
		return nil, fmt.Errorf("filter group %q is configured but not implemented in this chunk", activeGroup)
	}
	return append([]int(nil), group.Kinds...), nil
}

func ResolveBuildVersion(appVersion, buildVersion string) string {
	return runtimebootstrap.ResolveBuildVersion(appVersion, buildVersion)
}

func runMetricsLogger(ctx context.Context, log *slog.Logger, processor *live.Processor, every time.Duration) {
	if processor == nil || every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := processor.Snapshot()
			metrics.SetIngestSnapshot(snapshot.Valid, snapshot.Duplicate, snapshot.Invalid)
			log.Info(
				"ingest_metrics",
				"valid_total", snapshot.Valid,
				"duplicate_total", snapshot.Duplicate,
				"invalid_total", snapshot.Invalid,
			)
		}
	}
}

func runCheckpointFreshnessReporter(ctx context.Context, log *slog.Logger, pool *pgxpool.Pool, filterGroup string, every time.Duration) {
	if pool == nil || every <= 0 {
		return
	}
	filterGroup = strings.TrimSpace(filterGroup)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var latest *time.Time
			err := pool.QueryRow(ctx, `
				SELECT MAX(updated_at)
				FROM ingest_checkpoints
				WHERE mode = 'live' AND filter_group = $1
			`, filterGroup).Scan(&latest)
			if err != nil {
				log.Warn("ingest_checkpoint_freshness_query_failed", "error", err, "filter_group", filterGroup)
				continue
			}
			if latest == nil {
				metrics.SetIngestCheckpointFreshness("live", filterGroup, 1e9)
				continue
			}
			seconds := time.Since(latest.UTC()).Seconds()
			metrics.SetIngestCheckpointFreshness("live", filterGroup, seconds)
		}
	}
}
