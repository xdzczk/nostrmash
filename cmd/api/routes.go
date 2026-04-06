package main

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/api"
	"github.com/xdzczk/nostrmash/internal/api_primal"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

type routeDefinition struct {
	Pattern      string
	OwnsContract bool
	Target       routeTarget
	Handler      http.Handler
}

type routeTarget uint8

const (
	publicRoute routeTarget = iota
	adminRoute
)

func buildRouteDefinitions(
	pool *pgxpool.Pool,
	handlers api.Handlers,
	primalHandlers api_primal.Handlers,
	primalWS api_primal.WSGateway,
	adminHandlers api.AdminHandlers,
) []routeDefinition {
	return []routeDefinition{
		{
			Pattern:      "GET /health",
			OwnsContract: true,
			Target:       publicRoute,
			Handler:      http.HandlerFunc(api.Health),
		},
		{
			Pattern:      "GET /ready",
			OwnsContract: true,
			Target:       publicRoute,
			Handler:      api.Ready(pool),
		},
		{
			Pattern:      "GET /metrics",
			OwnsContract: true,
			Target:       publicRoute,
			Handler:      metrics.Handler(),
		},

		{Pattern: "GET /api/v1/events/{id}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetEventByID)},
		{Pattern: "POST /api/v1/events/batch", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.BatchGetEvents)},
		{Pattern: "GET /api/v1/events/{id}/seen-on", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetEventSeenOn)},
		{Pattern: "GET /api/v1/profiles/{pubkey}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetProfileByPubkey)},
		{Pattern: "POST /api/v1/profiles/batch", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.BatchGetProfiles)},
		{Pattern: "GET /api/v1/authors/{pubkey}/events", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetAuthorEvents)},
		{Pattern: "GET /api/v1/authors/{pubkey}/replies", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetAuthorReplies)},
		{Pattern: "GET /api/v1/events/{id}/counts", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetEventCounts)},
		{Pattern: "GET /api/v1/events/{id}/replies", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetEventReplies)},
		{Pattern: "GET /api/v1/events/{id}/ancestors", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetEventAncestors)},
		{Pattern: "GET /api/v1/threads/{eventId}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetThread)},
		{Pattern: "GET /api/v1/relays/health", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetRelaysHealth)},
		{Pattern: "GET /api/v1/contact-lists/{pubkey}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetContactList)},
		{Pattern: "GET /api/v1/relay-lists/{pubkey}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetRelayList)},
		{Pattern: "GET /api/v1/search", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.Search)},
		{Pattern: "GET /api/v1/users/{pubkey}/bookmarks", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetBookmarks)},
		{Pattern: "GET /api/v1/users/{pubkey}/highlights", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetHighlights)},
		{Pattern: "GET /api/v1/users/{pubkey}/long-form", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetLongForm)},
		{Pattern: "GET /api/v1/users/{pubkey}/zaps", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetZaps)},
		{Pattern: "GET /api/v1/users/{pubkey}/mentions", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetMentions)},
		{Pattern: "GET /api/v1/users/{pubkey}/followers", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(handlers.GetFollowers)},

		{Pattern: "GET /primal/v1/events/{id}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.GetEventByID)},
		{Pattern: "POST /primal/v1/events/batch", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.BatchGetEvents)},
		{Pattern: "GET /primal/v1/profiles/{pubkey}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.GetProfileByPubkey)},
		{Pattern: "POST /primal/v1/user_infos", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.BatchGetUserInfos)},
		{Pattern: "GET /primal/v1/threads/{eventId}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.GetThreadView)},
		{Pattern: "GET /primal/v1/authors/{pubkey}/events", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.GetAuthorEvents)},
		{Pattern: "GET /primal/v1/authors/{pubkey}/replies", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.GetAuthorReplies)},
		{Pattern: "GET /primal/v1/events/{id}/actions", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.GetEventActions)},
		{Pattern: "GET /primal/v1/contact-lists/{pubkey}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.GetContactList)},
		{Pattern: "GET /primal/v1/relay-lists/{pubkey}", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalHandlers.GetRelayList)},
		{Pattern: "GET /primal/ws", OwnsContract: true, Target: publicRoute, Handler: http.HandlerFunc(primalWS.Handle)},

		{Pattern: "GET /admin/v1/relays", OwnsContract: true, Target: adminRoute, Handler: http.HandlerFunc(adminHandlers.GetRelays)},
		{Pattern: "GET /admin/v1/jobs", OwnsContract: true, Target: adminRoute, Handler: http.HandlerFunc(adminHandlers.GetJobs)},
		{Pattern: "GET /admin/v1/invalid-events", OwnsContract: true, Target: adminRoute, Handler: http.HandlerFunc(adminHandlers.GetInvalidEvents)},
		{Pattern: "GET /admin/v1/rebuilds", OwnsContract: true, Target: adminRoute, Handler: http.HandlerFunc(adminHandlers.GetRebuilds)},
		{Pattern: "POST /admin/v1/rebuilds", OwnsContract: true, Target: adminRoute, Handler: http.HandlerFunc(adminHandlers.TriggerRebuild)},
		{Pattern: "GET /admin/v1/storage", OwnsContract: true, Target: adminRoute, Handler: http.HandlerFunc(adminHandlers.GetStorage)},
		{Pattern: "GET /admin/v1/system", OwnsContract: true, Target: adminRoute, Handler: http.HandlerFunc(adminHandlers.GetSystem)},
		{Pattern: "GET /admin/v1/derivation-versions", OwnsContract: true, Target: adminRoute, Handler: http.HandlerFunc(adminHandlers.GetDerivationVersions)},
	}
}

func registerDeclaredRoutes(publicMux, adminMux *http.ServeMux, defs []routeDefinition) {
	for _, route := range defs {
		switch route.Target {
		case publicRoute:
			publicMux.Handle(route.Pattern, route.Handler)
		case adminRoute:
			adminMux.Handle(route.Pattern, route.Handler)
		default:
			panic("unsupported route target")
		}
	}
}

func contractOwnedRoutes() []routeDefinition {
	return buildRouteDefinitions(nil, api.Handlers{}, api_primal.Handlers{}, api_primal.WSGateway{}, api.AdminHandlers{})
}
