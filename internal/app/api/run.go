// Package appapi contains the composition root for the API binary. Keeping the
// wiring here (rather than in cmd/api/main.go) keeps the binary entrypoint thin
// and makes the assembled server independently testable.
package appapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/xdzczk/nostrmash/internal/api"
	"github.com/xdzczk/nostrmash/internal/api_primal"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/meili"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/relaylookup"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/traceutil"
	"github.com/xdzczk/nostrmash/internal/trust"
)

// BuildInfo carries ldflags-injected build metadata from the binary entrypoint.
type BuildInfo struct {
	Version string
	Commit  string
	Time    string
}

// Run performs full API composition and blocks until ctx is cancelled, then
// gracefully shuts down. stop is invoked if the HTTP server fails to serve.
func Run(ctx context.Context, log *slog.Logger, build BuildInfo, stop func()) error {
	cfg, err := config.LoadAPI()
	if err != nil {
		return err
	}

	pool, err := store.OpenPool(ctx, cfg.Shared.Database.URL, cfg.Shared.Database.MaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	metrics.RegisterDBPool(pool)

	appVersion := envOrDefault("APP_VERSION", "dev")
	version := resolveBuildVersion(build.Version, appVersion)
	if err := traceutil.Init(ctx, cfg.Shared.ServiceName, "api", version, cfg.Shared.Environment); err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := traceutil.Shutdown(shutdownCtx); err != nil {
			log.Error("tracing_shutdown", "error", err)
		}
	}()
	metrics.RegisterBuildInfo("api", version, strings.TrimSpace(build.Commit), strings.TrimSpace(build.Time))
	metrics.RegisterDeploymentInfo("api", cfg.Shared.ServiceName, cfg.Shared.Environment)
	log.Info("build_info",
		"binary_role", "api",
		"version", version,
		"commit", strings.TrimSpace(build.Commit),
		"build_time", strings.TrimSpace(build.Time),
		"environment", cfg.Shared.Environment,
	)
	if err := store.Migrate(ctx, pool, appVersion); err != nil {
		return err
	}

	queryStore := store.NewPostgresStore(pool)
	// Compile-time proof that the production store implements the entire typed
	// read surface the query Service depends on. If a capability method is ever
	// removed from the store this fails to build instead of silently disabling
	// the feature at runtime.
	var _ query.FullStoreReader = queryStore
	meiliClient, err := meili.NewClient(meili.Config{
		Enabled:      cfg.Meilisearch.Enabled,
		URL:          cfg.Meilisearch.URL,
		MasterKey:    cfg.Meilisearch.MasterKey,
		SearchAPIKey: cfg.Meilisearch.SearchAPIKey,
	})
	if err != nil {
		return err
	}
	if meiliClient.Enabled() {
		if err := meiliClient.EnsureIndexesReady(ctx); err != nil {
			// Meilisearch coming up a few seconds after api is the
			// normal Coolify compose race. Search already falls back
			// to Postgres; crashing here is what produced the
			// dashboard "1x restarts" after every redeploy.
			log.Error("meilisearch_prepare", "error", err)
		}
		stats, statsErr := meiliClient.Stats(ctx)
		if statsErr != nil {
			log.Error("meilisearch_stats", "error", statsErr)
		} else {
			log.Info("meilisearch_ready", "healthy", stats.Healthy, "index_count", len(stats.Indexes))
		}
		go func() {
			// RunStartupFullSyncIfNeeded re-checks NeedsSync itself and
			// coordinates via a Postgres advisory lock so that when api and
			// worker restart together (e.g. a full redeploy) only one of
			// them actually streams the corpus into Meilisearch instead of
			// both doing it concurrently.
			syncCtx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
			defer cancel()
			syncStats, ran, syncErr := meiliClient.RunStartupFullSyncIfNeeded(syncCtx, pool, 1000)
			if syncErr != nil {
				log.Error("meilisearch_startup_sync_failed", "error", syncErr)
				return
			}
			if !ran {
				return
			}
			log.Info("meilisearch_indexes_stale", "action", "starting_full_sync")
			log.Info("meilisearch_startup_sync_complete",
				"profiles", syncStats.Profiles,
				"notes", syncStats.Notes,
				"documents", syncStats.Documents,
			)
		}()
	}
	meiliSearcher := meili.NewSearcher(meiliClient, queryStore)
	var fallbackReader query.FallbackStoreReader
	if cfg.RelayFallback.Enabled {
		maxFanout := cfg.RelayFallback.MaxFanout
		if policyMax := cfg.Shared.TrustPolicy.FallbackFetchMaxRelaysPerAttempt; policyMax > 0 && policyMax < maxFanout {
			maxFanout = policyMax
		}
		lookupClient := relaylookup.NewSplitClient(relaylookup.Config{
			EventURLs:   cfg.RelayFallback.URLs,
			ProfileURLs: cfg.RelayFallback.ProfileURLs,
			Timeout:     cfg.RelayFallback.Timeout,
			MaxFanout:   maxFanout,
		})
		fallbackReader = lookupClient
		log.Info(
			"relay_fallback_enabled",
			"event_relays", lookupClient.EventRelays(),
			"profile_relays", lookupClient.ProfileRelays(),
			"timeout", cfg.RelayFallback.Timeout.String(),
			"max_fanout", maxFanout,
			"use_registry", cfg.RelayFallback.UseRegistry,
		)
		if cfg.RelayFallback.UseRegistry {
			registry := relayregistry.NewStore(pool)
			refreshEventFallbackRelays(ctx, log, lookupClient, registry, cfg.RelayFallback.URLs, maxFanout)
			go runEventFallbackRefreshLoop(ctx, log, lookupClient, registry, cfg.RelayFallback.URLs, maxFanout, cfg.RelayFallback.RefreshInterval)
		}
	}
	personalizedRanker := trust.NewPersonalizedRanker(pool, cfg.Shared.TrustPolicy.PersonalizedMaxSeedFollows)
	// Optional cache for personalized trust rankings. Without it every
	// cache-miss request loads the full follow-graph adjacency and runs the
	// ranking iterations, so production should point TRUST_REDIS_URL at the
	// trust Redis instance. A missing/unreachable Redis only disables the
	// cache; it must never block API startup.
	if cfg.TrustRedis.URL != "" {
		redisOpts, err := redis.ParseURL(cfg.TrustRedis.URL)
		if err != nil {
			log.Error("trust_redis_parse_url", "error", err)
		} else {
			trustRedis := redis.NewClient(redisOpts)
			defer func() { _ = trustRedis.Close() }()
			if err := trustRedis.Ping(ctx).Err(); err != nil {
				log.Error("trust_redis_ping", "error", err)
			}
			// Attach even when the initial ping fails: Redis coming up a few
			// seconds after api is the normal compose race, and cache
			// reads/writes degrade gracefully per request.
			personalizedRanker = personalizedRanker.WithRedis(trustRedis, cfg.TrustRedis.KeyPrefix)
			log.Info("trust_personalized_cache_enabled", "key_prefix", cfg.TrustRedis.KeyPrefix)
		}
	}
	profilePersister := newMeiliSyncProfilePersister(
		query.AdaptFallbackProfilePersister(queryStore),
		meiliClient,
		pool,
	)
	eventPersister := newMeiliSyncEventPersister(
		query.AdaptFallbackEventPersister(queryStore),
		meiliClient,
		pool,
	)
	queryOptions := query.ServiceOptions{
		FallbackReader:                  query.AdaptFallbackReader(fallbackReader),
		FallbackProfilePersister:        profilePersister,
		FallbackEventPersister:          eventPersister,
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
		DiscoveryScoreBoostWeight:       cfg.Shared.TrustPolicy.DiscoveryScoreBoostWeight,
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
		MeilisearchSearcher: meiliSearcher,
		PersonalizedTrustRanker: personalizedTrustAdapter{
			inner: personalizedRanker,
		},
	}
	discoveryCacheEnabled := cfg.DiscoveryCache.Enabled
	handlers, err := api.NewHandlersWithOptions(queryStore, api.HandlersOptions{
		MaxBatchSize: cfg.HTTP.MaxBatchSize,
		Pool:         pool,
		QueryOptions: queryOptions,
		Hydration:    cfg.Hydration,
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
		return err
	}
	primalHandlers, err := api_primal.NewHandlersWithOptions(queryStore, api_primal.HandlersOptions{
		MaxBatchSize: cfg.HTTP.MaxBatchSize,
		QueryOptions: queryOptions,
	})
	if err != nil {
		return err
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
		return err
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
		MeiliClient:          meiliClient,
		Hydration:            cfg.Hydration,
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
	return nil
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

func resolveBuildVersion(buildVersion, appVersion string) string {
	if v := strings.TrimSpace(buildVersion); v != "" {
		return v
	}
	return strings.TrimSpace(appVersion)
}

func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
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
			tierBytes := make(map[string]float64, 3)
			for _, table := range snapshot.Tables {
				metrics.SetStorageTableRows(table.TableName, float64(table.RowCount))
				metrics.SetStorageTableBytes(table.TableName, float64(table.StorageBytes))
				metrics.SetStorageTableIndexBytes(table.TableName, float64(table.IndexBytes))
				tierBytes[api.StorageTableTier(table.TableName)] += float64(table.StorageBytes)
			}
			for _, tier := range []string{api.StorageTierCanonical, api.StorageTierDerived, api.StorageTierOperational} {
				metrics.SetStorageTierBytes(tier, tierBytes[tier])
			}
		}
	}
}

// meiliSyncProfilePersister wraps a FallbackProfilePersister to also mark the
// persisted profile for Meilisearch indexing via the worker sweeper's pending
// queue. Marking (a single-row upsert) replaces the previous inline SyncEvent:
// inline syncs emitted one-or-two single-document Meilisearch tasks per
// fallback hit, and at production index sizes each tiny task costs seconds of
// Meilisearch CPU, which kept the instance saturated. The sweeper drains the
// queue in large batches within one sweep interval, so search visibility is
// only marginally delayed.
type meiliSyncProfilePersister struct {
	inner query.FallbackProfilePersister
	meili *meili.Client
	pool  *pgxpool.Pool
}

func newMeiliSyncProfilePersister(
	inner query.FallbackProfilePersister,
	meili *meili.Client,
	pool *pgxpool.Pool,
) query.FallbackProfilePersister {
	if inner == nil {
		return nil
	}
	return &meiliSyncProfilePersister{inner: inner, meili: meili, pool: pool}
}

func (p *meiliSyncProfilePersister) PersistFallbackProfile(ctx context.Context, profile query.Profile) error {
	if err := p.inner.PersistFallbackProfile(ctx, profile); err != nil {
		return err
	}
	if p.meili != nil && p.meili.Enabled() && p.pool != nil && profile.MetadataEventID != "" {
		started := time.Now()
		if err := p.meili.MarkEventPendingSync(ctx, p.pool, profile.MetadataEventID); err != nil {
			metrics.ObserveMeiliSync("profile_fallback", "error", time.Since(started))
			slog.Warn("meilisearch_sync_failed", "source", "profile_fallback", "event_id", profile.MetadataEventID, "pubkey", profile.Pubkey, "error", err)
		} else {
			metrics.ObserveMeiliSync("profile_fallback", "success", time.Since(started))
		}
	}
	return nil
}

// meiliSyncEventPersister wraps a FallbackEventPersister to also mark the
// persisted event for Meilisearch indexing via the worker sweeper's pending
// queue (see meiliSyncProfilePersister for why marking replaced inline sync).
type meiliSyncEventPersister struct {
	inner query.FallbackEventPersister
	meili *meili.Client
	pool  *pgxpool.Pool
}

func newMeiliSyncEventPersister(
	inner query.FallbackEventPersister,
	meili *meili.Client,
	pool *pgxpool.Pool,
) query.FallbackEventPersister {
	if inner == nil {
		return nil
	}
	return &meiliSyncEventPersister{inner: inner, meili: meili, pool: pool}
}

func (p *meiliSyncEventPersister) PersistFallbackEvent(ctx context.Context, eventID string, raw json.RawMessage) error {
	if err := p.inner.PersistFallbackEvent(ctx, eventID, raw); err != nil {
		return err
	}
	if p.meili != nil && p.meili.Enabled() && p.pool != nil && eventID != "" {
		started := time.Now()
		if err := p.meili.MarkEventPendingSync(ctx, p.pool, eventID); err != nil {
			metrics.ObserveMeiliSync("event_fallback", "error", time.Since(started))
			slog.Warn("meilisearch_sync_failed", "source", "event_fallback", "event_id", eventID, "error", err)
		} else {
			metrics.ObserveMeiliSync("event_fallback", "success", time.Since(started))
		}
	}
	return nil
}

type personalizedTrustAdapter struct {
	inner *trust.PersonalizedRanker
}

func (a personalizedTrustAdapter) GetRanking(
	ctx context.Context,
	viewerPubkey string,
	limit int,
) ([]query.PersonalizedTrustScore, error) {
	if a.inner == nil {
		return nil, errors.New("personalized trust ranker is not configured")
	}
	rows, err := a.inner.GetRanking(ctx, viewerPubkey, limit)
	if err != nil {
		return nil, err
	}
	out := make([]query.PersonalizedTrustScore, 0, len(rows))
	for _, row := range rows {
		out = append(out, query.PersonalizedTrustScore{
			Pubkey: row.Pubkey,
			Score:  row.Score,
			Rank:   row.Rank,
			RunID:  row.RunID,
			Source: row.Source,
		})
	}
	return out, nil
}
