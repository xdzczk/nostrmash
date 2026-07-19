package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
	"github.com/xdzczk/nostrmash/internal/ingestor/live"
	"github.com/xdzczk/nostrmash/internal/ingestor/relay"
	"github.com/xdzczk/nostrmash/internal/jobs"
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

// backgroundDrainTimeout bounds how long shutdown waits for background ingest
// loops (relay reconciler/manager, metrics, trust fetch, discovery) to exit
// after their context is cancelled, before the caller closes the DB pool.
const backgroundDrainTimeout = 15 * time.Second

type runner struct {
	eventStore   *store.PostgresStore
	processor    *live.Processor
	kinds        []int
	trustedSet   *TrustedAuthorSet
	observations *ObservationBuffer
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
	pool, err := store.OpenPool(ctx, cfg.Shared.Database.URL, cfg.Shared.Database.MaxConns)
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

	// Always wire the trust gate. In "open" mode it only records shadow
	// metrics; in "trusted_only" it enforces. Keeping it always wired lets
	// operators watch trusted-set/gate metrics before flipping to enforce.
	trustedSet := NewTrustedAuthorSet(cfg.TrustGate.MaxHops)
	processor.SetTrustGate(cfg.TrustGate.Mode, trustedSet, eventStore)
	processor.SetBlockedAuthors(trustedSet)

	var observations *ObservationBuffer
	if cfg.AccountObservation.Enabled {
		observations = NewObservationBuffer(cfg.AccountObservation.MaxBufferKeys)
		processor.SetObservationSink(observations)
	}

	return runner{
		eventStore:   eventStore,
		processor:    processor,
		kinds:        kinds,
		trustedSet:   trustedSet,
		observations: observations,
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

	// loopCtx governs every background loop so shutdown can cancel and then
	// wait for them to exit before the caller closes the DB pool. waitLoops is
	// deferred (runs before cancelLoops via LIFO) so all return paths drain.
	loopCtx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()
	var loopWG sync.WaitGroup
	spawn := func(fn func(context.Context)) {
		loopWG.Add(1)
		go func() {
			defer loopWG.Done()
			fn(loopCtx)
		}()
	}
	waitLoops := func() {
		cancelLoops()
		done := make(chan struct{})
		go func() {
			loopWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(backgroundDrainTimeout):
			log.Warn("ingestor_background_loops_drain_timeout", "drain_timeout", backgroundDrainTimeout.String())
		}
	}
	defer waitLoops()

	runtimebootstrap.StartMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)
	spawn(func(c context.Context) { runMetricsLogger(c, log, runner.processor, 30*time.Second) })
	spawn(func(c context.Context) {
		runTrustedAuthorSetRefreshLoop(c, log, runner.trustedSet, runner.eventStore, cfg.TrustGate.Mode, cfg.TrustGate.MaxHops, cfg.TrustGate.RefreshInterval)
	})
	if runner.observations != nil {
		spawn(func(c context.Context) {
			RunObservationFlushLoop(c, log, runner.eventStore, runner.observations, cfg.AccountObservation.FlushInterval)
		})
	}

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
	spawn(func(c context.Context) { runCheckpointFreshnessReporter(c, log, pool, cfg.Relay.ActiveFilterGroup, 30*time.Second) })

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
			handlerWithPool(runner.processor.Handle, jobs.WorkerPoolBackfill),
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
		spawn(func(c context.Context) {
			runTrustTargetedFetchLoop(
				c,
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
				handlerWithPool(runner.processor.Handle, jobs.WorkerPoolBackfill),
			)
		})
		log.Info(
			"trust_fetch_started",
			"max_tracked_pubkeys", cfg.TrustFetch.MaxTrackedPubkeys,
			"max_selected_per_cycle", cfg.TrustFetch.MaxSelectedPerCycle,
			"refresh_interval", cfg.TrustFetch.RefreshInterval.String(),
		)
	}

	if cfg.AuthorMetadataDiscovery.Enabled {
		spawn(func(c context.Context) {
			runAuthorMetadataDiscoveryLoop(
				c,
				log,
				runner.eventStore,
				cfg.AuthorMetadataDiscovery,
				prioritizedRelays,
				backfill.WebsocketFetcher{
					Log:            log,
					ConnectTimeout: cfg.Backfill.ConnectTimeout,
					IdleTimeout:    cfg.Backfill.IdleTimeout,
				},
				handlerWithPool(runner.processor.Handle, jobs.WorkerPoolBackfill),
			)
		})
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
			handlerWithPool(runner.processor.Handle, jobs.WorkerPoolLive),
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
		spawn(reconciler.Run)
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
			handlerWithPool(runner.processor.Handle, jobs.WorkerPoolLive),
			log,
		)
		if err != nil {
			log.Error("relay_config", "error", err)
			return err
		}
		spawn(relayManager.Start)
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
		"gated_total", final.Gated,
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

// handlerWithPool wraps a relay event handler so that any derivation jobs
// enqueued while persisting an event are routed to the supplied worker pool.
// This lets live ingest bypass historical backlog by writing into a separate
// queue lane.
func handlerWithPool(
	handler func(ctx context.Context, relayURL string, payload []byte) error,
	workerPool string,
) func(ctx context.Context, relayURL string, payload []byte) error {
	if handler == nil || strings.TrimSpace(workerPool) == "" {
		return handler
	}
	return func(ctx context.Context, relayURL string, payload []byte) error {
		return handler(jobs.WithWorkerPool(ctx, workerPool), relayURL, payload)
	}
}

// runTrustedAuthorSetRefreshLoop periodically reloads the in-memory trusted
// author set from trust_graph_snapshot and publishes set-size/loaded/age
// metrics. It refreshes once immediately so the gate has data shortly after
// startup. On refresh failure the last-good set is retained (see
// TrustedAuthorSet.Refresh) and the age metric keeps growing to surface
// staleness.
func runTrustedAuthorSetRefreshLoop(
	ctx context.Context,
	log *slog.Logger,
	set *TrustedAuthorSet,
	loader trustedAuthorLoader,
	mode string,
	maxHops int,
	every time.Duration,
) {
	if set == nil || loader == nil || every <= 0 {
		return
	}
	log.Info(
		"ingest_trusted_set_refresh_started",
		"mode", mode,
		"max_hops", maxHops,
		"refresh_interval", every.String(),
	)
	refresh := func() {
		if err := set.Refresh(ctx, loader); err != nil {
			log.Warn("ingest_trusted_set_refresh_failed", "error", err)
		}
		metrics.SetIngestTrustedSetLoaded(set.Loaded())
		metrics.SetIngestTrustedSetSize(set.Size())
		if last := set.LastRefreshAt(); !last.IsZero() {
			metrics.SetIngestTrustedSetAge(time.Since(last).Seconds())
		}
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
				"gated_total", snapshot.Gated,
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
