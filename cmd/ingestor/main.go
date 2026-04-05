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

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
	"github.com/xdzczk/nostrmash/internal/ingestor/live"
	"github.com/xdzczk/nostrmash/internal/ingestor/relay"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/replay"
	"github.com/xdzczk/nostrmash/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("ingestor")

	cfg, err := config.Load("ingestor")
	if err != nil {
		log.Error("config", "error", err)
		os.Exit(1)
	}

	pool, err := store.OpenPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("db_connect", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	appVersion := os.Getenv("APP_VERSION")
	if appVersion == "" {
		appVersion = "dev"
	}
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
	runMetricsEndpoint(ctx, log, cfg.MetricsAddr)

	go runMetricsLogger(ctx, log, processor, 30*time.Second)

	if cfg.Mode == "replay" {
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
		"mode", cfg.Mode,
		"filter_group", cfg.Relay.ActiveFilterGroup,
		"relay_count", len(cfg.Relay.URLs),
		"relay_urls", cfg.Relay.URLs,
		"resume_checkpoint_enabled", true,
		"bootstrap_lookback_seconds", cfg.Relay.LiveBootstrapLookbackSeconds,
		"resume_overlap_seconds", cfg.Relay.LiveResumeOverlapSeconds,
	)

	if cfg.Backfill.Enabled {
		backfillRunner, err := backfill.NewRunner(
			log,
			backfill.Config{
				Relays:       cfg.Relay.URLs,
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
		log.Info("backfill_completed", "relay_count", len(cfg.Relay.URLs))
	}

	relayManager, err := relay.NewManager(
		relay.Config{
			Relays:         cfg.Relay.URLs,
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
	log.Info("relay_manager_started", "relay_count", len(cfg.Relay.URLs))

	<-ctx.Done()
	if checkpointTracker != nil {
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
