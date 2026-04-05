package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xdzczk/nostrmash/internal/api"
	"github.com/xdzczk/nostrmash/internal/api_primal"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("api")

	cfg, err := config.Load("api")
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

	queryStore := store.NewPostgresStore(pool)
	handlers := api.NewHandlers(queryStore, cfg.APIMaxBatchSize)
	primalHandlers := api_primal.NewHandlers(queryStore, cfg.APIMaxBatchSize)
	wsLog := logging.New("api_primal_ws")
	primalWS := api_primal.NewWSGateway(queryStore, api_primal.WSGatewayOptions{
		MaxSubscriptions:  cfg.PrimalWSMaxSubscriptions,
		RequestTimeout:    cfg.PrimalWSRequestTimeout,
		MaxMessageBytes:   cfg.PrimalWSMaxMessageBytes,
		MaxReqPerMinute:   cfg.PrimalWSMaxReqPerMinute,
		MaxDMReqPerMinute: cfg.PrimalWSDMCompatRateLimitRPM,
		AllowedOrigins:    cfg.PrimalWSAllowedOrigins,
		AllowAnyOrigin:    cfg.PrimalWSAllowAnyOrigin,
		Logger:            wsLog,
	})
	adminService := api.NewAdminService(pool, derivation.NewHandlers(pool), api.AdminServiceOptions{
		ServiceName:      cfg.ServiceName,
		Environment:      cfg.Environment,
		AppVersion:       appVersion,
		StartedAt:        time.Now().UTC(),
		ConfiguredRelays: cfg.Relay.URLs,
		DisabledRelays:   cfg.Relay.Disabled,
	})
	adminHandlers := api.NewAdminHandlers(adminService)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", api.Health)
	mux.HandleFunc("GET /ready", api.Ready(pool))
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /api/v1/events/{id}", handlers.GetEventByID)
	mux.HandleFunc("POST /api/v1/events/batch", handlers.BatchGetEvents)
	mux.HandleFunc("GET /api/v1/events/{id}/seen-on", handlers.GetEventSeenOn)
	mux.HandleFunc("GET /api/v1/profiles/{pubkey}", handlers.GetProfileByPubkey)
	mux.HandleFunc("POST /api/v1/profiles/batch", handlers.BatchGetProfiles)
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/events", handlers.GetAuthorEvents)
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/replies", handlers.GetAuthorReplies)
	mux.HandleFunc("GET /api/v1/events/{id}/counts", handlers.GetEventCounts)
	mux.HandleFunc("GET /api/v1/events/{id}/replies", handlers.GetEventReplies)
	mux.HandleFunc("GET /api/v1/events/{id}/ancestors", handlers.GetEventAncestors)
	mux.HandleFunc("GET /api/v1/threads/{eventId}", handlers.GetThread)
	mux.HandleFunc("GET /api/v1/relays/health", handlers.GetRelaysHealth)
	mux.HandleFunc("GET /api/v1/contact-lists/{pubkey}", handlers.GetContactList)
	mux.HandleFunc("GET /api/v1/relay-lists/{pubkey}", handlers.GetRelayList)
	mux.HandleFunc("GET /api/v1/search", handlers.Search)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/bookmarks", handlers.GetBookmarks)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/highlights", handlers.GetHighlights)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/long-form", handlers.GetLongForm)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/zaps", handlers.GetZaps)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/mentions", handlers.GetMentions)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/followers", handlers.GetFollowers)
	mux.HandleFunc("GET /primal/v1/events/{id}", primalHandlers.GetEventByID)
	mux.HandleFunc("POST /primal/v1/events/batch", primalHandlers.BatchGetEvents)
	mux.HandleFunc("GET /primal/v1/profiles/{pubkey}", primalHandlers.GetProfileByPubkey)
	mux.HandleFunc("POST /primal/v1/user_infos", primalHandlers.BatchGetUserInfos)
	mux.HandleFunc("GET /primal/v1/threads/{eventId}", primalHandlers.GetThreadView)
	mux.HandleFunc("GET /primal/v1/authors/{pubkey}/events", primalHandlers.GetAuthorEvents)
	mux.HandleFunc("GET /primal/v1/authors/{pubkey}/replies", primalHandlers.GetAuthorReplies)
	mux.HandleFunc("GET /primal/v1/events/{id}/actions", primalHandlers.GetEventActions)
	mux.HandleFunc("GET /primal/v1/contact-lists/{pubkey}", primalHandlers.GetContactList)
	mux.HandleFunc("GET /primal/v1/relay-lists/{pubkey}", primalHandlers.GetRelayList)
	mux.HandleFunc("GET /primal/ws", primalWS.Handle)

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("GET /admin/v1/relays", adminHandlers.GetRelays)
	adminMux.HandleFunc("GET /admin/v1/jobs", adminHandlers.GetJobs)
	adminMux.HandleFunc("GET /admin/v1/invalid-events", adminHandlers.GetInvalidEvents)
	adminMux.HandleFunc("GET /admin/v1/rebuilds", adminHandlers.GetRebuilds)
	adminMux.HandleFunc("POST /admin/v1/rebuilds", adminHandlers.TriggerRebuild)
	adminMux.HandleFunc("GET /admin/v1/storage", adminHandlers.GetStorage)
	adminMux.HandleFunc("GET /admin/v1/system", adminHandlers.GetSystem)
	adminMux.HandleFunc("GET /admin/v1/derivation-versions", adminHandlers.GetDerivationVersions)
	mux.Handle("/admin/", api.RequireBearerToken(strings.TrimSpace(cfg.AdminBearerToken), adminMux))

	var handler http.Handler = mux
	handler = api.WithHTTPRateLimit(api.HTTPRateLimitOptions{
		DefaultRPM:   cfg.HTTPRateLimitRPM,
		DefaultBurst: cfg.HTTPRateLimitBurst,
		SearchRPM:    cfg.HTTPSearchRateLimitRPM,
		BatchRPM:     cfg.HTTPBatchRateLimitRPM,
	}, handler)
	handler = api.LogRequests(log, handler)
	handler = api.WithRequestID(handler)

	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http_server", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown_started")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http_shutdown", "error", err)
	}
	log.Info("shutdown_complete")
}
