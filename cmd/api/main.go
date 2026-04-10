package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/api"
	"github.com/xdzczk/nostrmash/internal/api_primal"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/relaylookup"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
	"github.com/xdzczk/nostrmash/internal/trust"
)

var (
	buildVersion = ""
	buildCommit  = "unknown"
	buildTime    = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("api")

	cfg, err := config.LoadAPI()
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
	if err := traceutil.Init(ctx, cfg.Shared.ServiceName, "api", version, cfg.Shared.Environment); err != nil {
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
	metrics.RegisterBuildInfo("api", version, strings.TrimSpace(buildCommit), strings.TrimSpace(buildTime))
	metrics.RegisterDeploymentInfo("api", cfg.Shared.ServiceName, cfg.Shared.Environment)
	log.Info("build_info",
		"binary_role", "api",
		"version", version,
		"commit", strings.TrimSpace(buildCommit),
		"build_time", strings.TrimSpace(buildTime),
		"environment", cfg.Shared.Environment,
	)
	if err := store.Migrate(ctx, pool, appVersion); err != nil {
		log.Error("migrate", "error", err)
		os.Exit(1)
	}

	queryStore := store.NewPostgresStore(pool)
	var fallbackReader any
	if cfg.RelayFallback.Enabled {
		maxFanout := cfg.RelayFallback.MaxFanout
		if policyMax := cfg.Shared.TrustPolicy.FallbackFetchMaxRelaysPerAttempt; policyMax > 0 && policyMax < maxFanout {
			maxFanout = policyMax
		}
		fallbackReader = relaylookup.NewClient(cfg.RelayFallback.URLs, cfg.RelayFallback.Timeout, maxFanout)
		log.Info(
			"relay_fallback_enabled",
			"relay_count", len(cfg.RelayFallback.URLs),
			"timeout", cfg.RelayFallback.Timeout.String(),
			"max_fanout", maxFanout,
		)
	}
	queryOptions := query.ServiceOptions{
		FallbackReader:                  fallbackReader,
		FallbackProfilePersister:        query.AdaptFallbackProfilePersister(queryStore),
		FallbackEventPersister:          query.AdaptFallbackEventPersister(queryStore),
		FallbackFetchTrustMode:          cfg.Shared.TrustPolicy.FallbackFetchMode,
		FallbackFetchMinimumScore:       cfg.Shared.TrustPolicy.MinimumScore,
		FallbackFetchMaxHops:            cfg.Shared.TrustPolicy.MaxHops,
		FallbackFetchMaxAttempts:        cfg.Shared.TrustPolicy.FallbackFetchMaxAttempts,
		FallbackFetchMaxTimeBudget:      cfg.Shared.TrustPolicy.FallbackFetchMaxTimeBudget,
		FallbackFetchAllowDirectLookup:  &cfg.Shared.TrustPolicy.FallbackFetchAllowDirectLookup,
		DiscoveryCandidateTrustMode:     cfg.Shared.TrustPolicy.DiscoveryCandidateMode,
		SearchRankingTrustMode:          cfg.Shared.TrustPolicy.SearchRankingMode,
		DiscoveryCandidateMinimumScore:  cfg.Shared.TrustPolicy.MinimumScore,
		DiscoveryCandidateMaxHops:       cfg.Shared.TrustPolicy.MaxHops,
		DiscoveryProjectionMaxStaleness: cfg.Shared.TrustPolicy.RefreshInterval,
		TrustRetentionHooks: query.TrustRetentionHooks{
			Mode: cfg.Shared.TrustPolicy.RetentionPolicyMode,
			DiscoveryCache: query.TrustRetentionHook{
				Owner:            "query.discovery_cache",
				Enabled:          cfg.Shared.TrustPolicy.RetentionHooks.DiscoveryCache.Enabled,
				TrustedHorizon:   cfg.Shared.TrustPolicy.RetentionHooks.DiscoveryCache.TrustedHorizon,
				UntrustedHorizon: cfg.Shared.TrustPolicy.RetentionHooks.DiscoveryCache.UntrustedHorizon,
			},
			DiscoveryCandidateProjection: query.TrustRetentionHook{
				Owner:            "query.discovery_candidate_projection",
				Enabled:          cfg.Shared.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates.Enabled,
				TrustedHorizon:   cfg.Shared.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates.TrustedHorizon,
				UntrustedHorizon: cfg.Shared.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates.UntrustedHorizon,
			},
			LowValueEnrichmentState: query.TrustRetentionHook{
				Owner:            "query.low_value_enrichment_state",
				Enabled:          cfg.Shared.TrustPolicy.RetentionHooks.LowValueEnrichmentState.Enabled,
				TrustedHorizon:   cfg.Shared.TrustPolicy.RetentionHooks.LowValueEnrichmentState.TrustedHorizon,
				UntrustedHorizon: cfg.Shared.TrustPolicy.RetentionHooks.LowValueEnrichmentState.UntrustedHorizon,
			},
			FallbackTransientMetadata: query.TrustRetentionHook{
				Owner:            "query.fallback_transient_metadata",
				Enabled:          cfg.Shared.TrustPolicy.RetentionHooks.FallbackTransientMetadata.Enabled,
				TrustedHorizon:   cfg.Shared.TrustPolicy.RetentionHooks.FallbackTransientMetadata.TrustedHorizon,
				UntrustedHorizon: cfg.Shared.TrustPolicy.RetentionHooks.FallbackTransientMetadata.UntrustedHorizon,
			},
		},
	}
	discoveryCacheEnabled := cfg.DiscoveryCache.Enabled
	handlers, err := api.NewHandlersWithOptions(queryStore, api.HandlersOptions{
		MaxBatchSize: cfg.HTTP.MaxBatchSize,
		QueryOptions: queryOptions,
		DiscoveryCache: &api.DiscoveryCacheOptions{
			Enabled:        &discoveryCacheEnabled,
			MaxEntries:     cfg.DiscoveryCache.MaxEntries,
			BundleTTL:      cfg.DiscoveryCache.BundleTTL,
			DiscoveryTTL:   cfg.DiscoveryCache.DiscoveryTTL,
			SuggestionTTL:  cfg.DiscoveryCache.SuggestionTTL,
			TrendingTTL:    cfg.DiscoveryCache.TrendingTTL,
			StatsTTL:       cfg.DiscoveryCache.StatsTTL,
			PublicStatsTTL: cfg.DiscoveryCache.PublicStatsTTL,
		},
	})
	if err != nil {
		log.Error("query_service_init", "surface", "api", "error", err)
		os.Exit(1)
	}
	primalHandlers, err := api_primal.NewHandlersWithOptions(queryStore, api_primal.HandlersOptions{
		MaxBatchSize: cfg.HTTP.MaxBatchSize,
		QueryOptions: queryOptions,
	})
	if err != nil {
		log.Error("query_service_init", "surface", "api_primal_http", "error", err)
		os.Exit(1)
	}
	wsLog := logging.New("api_primal_ws")
	primalWS, err := api_primal.NewWSGateway(queryStore, api_primal.WSGatewayOptions{
		MaxSubscriptions:  cfg.PrimalWS.MaxSubscriptions,
		RequestTimeout:    cfg.PrimalWS.RequestTimeout,
		MaxMessageBytes:   cfg.PrimalWS.MaxMessageBytes,
		MaxReqPerMinute:   cfg.PrimalWS.MaxReqPerMinute,
		MaxDMReqPerMinute: cfg.PrimalWS.DMCompatRateLimitRPM,
		AllowedOrigins:    cfg.PrimalWS.AllowedOrigins,
		AllowAnyOrigin:    cfg.PrimalWS.AllowAnyOrigin,
		Logger:            wsLog,
		QueryOptions:      queryOptions,
	})
	if err != nil {
		log.Error("query_service_init", "surface", "api_primal_ws", "error", err)
		os.Exit(1)
	}
	adminService := api.NewAdminService(pool, derivation.NewHandlers(pool), trust.NewRuntime(pool, false, true), api.AdminServiceOptions{
		ServiceName:          cfg.Shared.ServiceName,
		Environment:          cfg.Shared.Environment,
		AppVersion:           appVersion,
		StartedAt:            time.Now().UTC(),
		ConfiguredRelays:     cfg.Relay.URLs,
		DisabledRelays:       cfg.Relay.Disabled,
		DiscoveryTrustMode:   cfg.Shared.TrustPolicy.DiscoveryCandidateMode,
		SearchTrustMode:      cfg.Shared.TrustPolicy.SearchRankingMode,
		TrustRefreshInterval: cfg.Shared.TrustPolicy.RefreshInterval,
	})
	adminHandlers := api.NewAdminHandlers(adminService)

	mux := http.NewServeMux()
	adminMux := http.NewServeMux()
	registerDeclaredRoutes(mux, adminMux, buildRouteDefinitions(pool, handlers, primalHandlers, primalWS, adminHandlers))
	mux.Handle("/admin/", api.RequireBearerToken(strings.TrimSpace(cfg.HTTP.AdminBearerToken), adminMux))

	var handler http.Handler = mux
	handler = api.WithPublicRequestGuards(api.PublicRequestGuardOptions{
		MaxResultLimit:          cfg.HTTP.PublicMaxResultLimit,
		MaxPageSize:             cfg.HTTP.PublicMaxPageSize,
		MaxPageOffset:           cfg.HTTP.PublicMaxPageOffset,
		MaxSearchWindowHours:    cfg.HTTP.PublicMaxSearchWindowHrs,
		MaxDiscoveryWindowHours: cfg.HTTP.PublicMaxDiscoveryWindowHrs,
	}, handler)
	handler = api.WithHTTPRateLimit(api.HTTPRateLimitOptions{
		DefaultRPM:     cfg.HTTP.RateLimitRPM,
		DefaultBurst:   cfg.HTTP.RateLimitBurst,
		SearchRPM:      cfg.HTTP.SearchRateLimitRPM,
		BatchRPM:       cfg.HTTP.BatchRateLimitRPM,
		DiscoveryRPM:   cfg.HTTP.DiscoveryRateLimitRPM,
		SuggestRPM:     cfg.HTTP.SuggestRateLimitRPM,
		PublicStatsRPM: cfg.HTTP.PublicStatsRateLimitRPM,
	}, handler)
	handler = api.LogRequests(log, handler)
	handler = api.WithRequestID(handler)
	handler = api.WithPanicRecovery(log, handler)

	srv := &http.Server{
		Addr:         cfg.HTTP.Addr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", cfg.HTTP.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http_server", "error", err)
			stop()
		}
	}()
	go runStorageMetricsReporter(ctx, log, pool, 2*time.Minute, api.TrackedStorageTables())
	runDebugEndpoint(ctx, log, cfg.Shared.Observability.DebugAddr, cfg.HTTP.AdminBearerToken)

	<-ctx.Done()
	log.Info("shutdown_started")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http_shutdown", "error", err)
	}
	log.Info("shutdown_complete")
}

func runDebugEndpoint(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, addr, adminBearerToken string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	adminBearerToken = strings.TrimSpace(adminBearerToken)
	if adminBearerToken == "" {
		log.Info("debug_disabled", "reason", "missing_admin_bearer_token")
		return
	}

	mux := http.NewServeMux()
	registerPprofHandlers(mux)
	srv := &http.Server{
		Addr:         addr,
		Handler:      api.RequireBearerToken(adminBearerToken, mux),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Info("debug_listening", "addr", addr, "surface", "pprof", "auth", "admin_bearer")
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

func runStorageMetricsReporter(
	ctx context.Context,
	log interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	},
	pool *pgxpool.Pool,
	every time.Duration,
	tables []string,
) {
	if every <= 0 || len(tables) == 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot, err := store.CollectStorageStats(ctx, pool, tables, store.StorageStatsOptions{
				ExactRowCountMaxBytes: 0, // metrics path stays cheap on large tables
			})
			if err != nil {
				log.Error("storage_metrics_collect_failed", "error", err)
				continue
			}
			metrics.SetStorageDatabaseBytes(float64(snapshot.DatabaseBytes))
			for _, table := range snapshot.Tables {
				metrics.SetStorageTableRows(table.TableName, float64(table.RowCount))
				metrics.SetStorageTableBytes(table.TableName, float64(table.StorageBytes))
			}
		}
	}
}
