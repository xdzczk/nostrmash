package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
	"github.com/xdzczk/nostrmash/internal/ingestor/live"
	"github.com/xdzczk/nostrmash/internal/ingestor/relay"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/replay"
	"github.com/xdzczk/nostrmash/internal/store"
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

	log := logging.New("ingestor")

	cfg, err := config.LoadIngestor()
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
	if err := traceutil.Init(ctx, cfg.Shared.ServiceName, "ingestor", version, cfg.Shared.Environment); err != nil {
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
	metrics.RegisterBuildInfo("ingestor", version, strings.TrimSpace(buildCommit), strings.TrimSpace(buildTime))
	metrics.RegisterDeploymentInfo("ingestor", cfg.Shared.ServiceName, cfg.Shared.Environment)
	log.Info("build_info",
		"binary_role", "ingestor",
		"version", version,
		"commit", strings.TrimSpace(buildCommit),
		"build_time", strings.TrimSpace(buildTime),
		"environment", cfg.Shared.Environment,
	)
	if err := store.Migrate(ctx, pool, appVersion); err != nil {
		log.Error("migrate", "error", err)
		os.Exit(1)
	}

	kinds, err := resolveLiveKinds(cfg.Relay)
	if err != nil {
		log.Error("ingestor_filter_group", "error", err)
		os.Exit(1)
	}

	eventStore := store.NewPostgresStore(pool)
	processor, err := live.NewProcessor(log, eventStore, nostr.Options{})
	if err != nil {
		log.Error("ingestor_processor", "error", err)
		os.Exit(1)
	}
	var checkpointTracker *live.CheckpointTracker
	var sinceResolver relay.SinceResolver
	runMetricsEndpoint(ctx, log, cfg.Shared.Observability.MetricsAddr)

	go runMetricsLogger(ctx, log, processor, 30*time.Second)

	if cfg.Runtime.Mode == "replay" {
		replayRunner, err := replay.NewRunner(log, pool, nostr.Options{})
		if err != nil {
			log.Error("replay_runner_init", "error", err)
			os.Exit(1)
		}
		result, err := replayRunner.ReplayFixturePath(ctx, cfg.Replay.FixturePath)
		if err != nil {
			log.Error("replay_failed", "error", err)
			os.Exit(1)
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
		return
	}

	checkpointTracker, err = live.NewCheckpointTracker(
		log,
		eventStore,
		cfg.Relay.ActiveFilterGroup,
		5*time.Second,
	)
	if err != nil {
		log.Error("live_checkpoint_tracker", "error", err)
		os.Exit(1)
	}
	processor.SetCheckpointWriter(checkpointTracker)
	sinceResolver, err = live.NewResumeSinceResolver(
		eventStore,
		cfg.Relay.ActiveFilterGroup,
		cfg.Relay.LiveBootstrapLookbackSeconds,
		cfg.Relay.LiveResumeOverlapSeconds,
	)
	if err != nil {
		log.Error("live_since_resolver", "error", err)
		os.Exit(1)
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
		backfillRunner, err := backfill.NewRunner(
			log,
			backfill.Config{
				Relays:       prioritizedRelays,
				FilterGroup:  cfg.Relay.ActiveFilterGroup,
				Kinds:        kinds,
				Mode:         cfg.Backfill.Mode,
				Since:        cfg.Backfill.Since,
				Until:        cfg.Backfill.Until,
				PageLimit:    cfg.Backfill.PageLimit,
				EmptyPageMax: cfg.Backfill.EmptyPageMax,
			},
			eventStore,
			backfill.WebsocketFetcher{
				Log:            log,
				ConnectTimeout: cfg.Backfill.ConnectTimeout,
				IdleTimeout:    cfg.Backfill.IdleTimeout,
			},
			processor.Handle,
		)
		if err != nil {
			log.Error("backfill_runner_config", "error", err)
			os.Exit(1)
		}
		if err := backfillRunner.Run(ctx); err != nil {
			log.Error("backfill_runner", "error", err)
			os.Exit(1)
		}
		log.Info("backfill_completed", "relay_count", len(prioritizedRelays))
	}

	if cfg.TrustFetch.Enabled {
		go runTrustTargetedFetchLoop(
			ctx,
			log,
			eventStore,
			cfg.TrustPrioritization,
			cfg.TrustFetch,
			prioritizedRelays,
			backfill.WebsocketFetcher{
				Log:            log,
				ConnectTimeout: cfg.Backfill.ConnectTimeout,
				IdleTimeout:    cfg.Backfill.IdleTimeout,
			},
			processor.Handle,
		)
		log.Info(
			"trust_fetch_started",
			"max_tracked_pubkeys", cfg.TrustFetch.MaxTrackedPubkeys,
			"max_selected_per_cycle", cfg.TrustFetch.MaxSelectedPerCycle,
			"refresh_interval", cfg.TrustFetch.RefreshInterval.String(),
		)
	}

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
			Kinds:         kinds,
			FilterGroup:   cfg.Relay.ActiveFilterGroup,
			SinceResolver: sinceResolver,
		},
		processor.Handle,
		log,
	)
	if err != nil {
		log.Error("relay_config", "error", err)
		os.Exit(1)
	}
	go relayManager.Start(ctx)
	log.Info("relay_manager_started", "relay_count", len(prioritizedRelays))

	<-ctx.Done()
	if checkpointTracker != nil {
		for _, relayURL := range cfg.Relay.URLs {
			if err := checkpointTracker.SetRelayStatus(context.Background(), relayURL, relay.StateDisconnected, ""); err != nil {
				log.Warn("live_checkpoint_disconnect_shutdown", "relay_url", relayURL, "error", err)
			}
		}
		if err := checkpointTracker.FlushAll(context.Background()); err != nil {
			log.Warn("live_checkpoint_flush_shutdown", "error", err)
		}
	}
	final := processor.Snapshot()
	log.Info(
		"ingest_metrics_final",
		"valid_total", final.Valid,
		"duplicate_total", final.Duplicate,
		"invalid_total", final.Invalid,
	)
	log.Info("shutdown_complete")
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

func sortRelaysByWeights(normalized []string, baseOrder map[string]int, weights map[string]float64) []string {
	return store.SortRelaysByWeights(normalized, baseOrder, weights)
}

func resolveLiveKinds(cfg config.RelayConfig) ([]int, error) {
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

func runMetricsEndpoint(ctx context.Context, log *slog.Logger, addr string) {
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

func resolveBuildVersion(appVersion string) string {
	if v := strings.TrimSpace(buildVersion); v != "" {
		return v
	}
	return strings.TrimSpace(appVersion)
}
