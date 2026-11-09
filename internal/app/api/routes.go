package appapi

import (
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/api"
	"github.com/xdzczk/nostrmash/internal/api_primal"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

type routeDefinition struct {
	Pattern      string
	OwnsContract bool
	Handler      http.Handler
	register     routeRegistrar
}

type routeRegistrar func(publicMux, adminMux *http.ServeMux, pattern string, handler http.Handler)

func registerPublicRoute(publicMux, _ *http.ServeMux, pattern string, handler http.Handler) {
	publicMux.Handle(pattern, handler)
}

func registerAdminRoute(_ *http.ServeMux, adminMux *http.ServeMux, pattern string, handler http.Handler) {
	adminMux.Handle(pattern, handler)
}

func newPublicRoute(pattern string, ownsContract bool, handler http.Handler) routeDefinition {
	return newRouteDefinition(pattern, ownsContract, handler, registerPublicRoute)
}

func newAdminRoute(pattern string, ownsContract bool, handler http.Handler) routeDefinition {
	return newRouteDefinition(pattern, ownsContract, handler, registerAdminRoute)
}

func newRouteDefinition(
	pattern string,
	ownsContract bool,
	handler http.Handler,
	register routeRegistrar,
) routeDefinition {
	if strings.TrimSpace(pattern) == "" {
		panic("route pattern is required")
	}
	if handler == nil {
		panic("route handler is required")
	}
	if register == nil {
		panic("route registrar is required")
	}
	return routeDefinition{
		Pattern:      pattern,
		OwnsContract: ownsContract,
		Handler:      handler,
		register:     register,
	}
}

func buildRouteDefinitions(
	pool *pgxpool.Pool,
	handlers api.Handlers,
	primalHandlers api_primal.Handlers,
	primalWS api_primal.WSGateway,
	primalWSV1 api_primal.WSGateway,
	v1Enabled bool,
	adminHandlers api.AdminHandlers,
) []routeDefinition {
	v1Handler := http.Handler(http.HandlerFunc(primalWSV1.Handle))
	if !v1Enabled {
		v1Handler = http.NotFoundHandler()
	}
	publicReads := api.NewPublicReadCache(api.PublicReadCacheConfig{})
	cachedRead := func(handler http.HandlerFunc) http.Handler {
		return publicReads.Handler(handler)
	}
	return []routeDefinition{
		newPublicRoute("GET /health", true, http.HandlerFunc(api.Health)),
		newPublicRoute("GET /ready", true, api.Ready(pool)),
		newPublicRoute("GET /metrics", true, metrics.Handler()),

		newPublicRoute("GET /api/v1/events/{id}", true, http.HandlerFunc(handlers.GetEventByID)),
		newPublicRoute("POST /api/v1/events/batch", true, http.HandlerFunc(handlers.BatchGetEvents)),
		newPublicRoute("GET /api/v1/events/{id}/seen-on", true, cachedRead(handlers.GetEventSeenOn)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}", true, http.HandlerFunc(handlers.GetProfileByPubkey)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}/topics", true, http.HandlerFunc(handlers.GetProfileTopics)),
		newPublicRoute("POST /api/v1/profiles/batch", true, http.HandlerFunc(handlers.BatchGetProfiles)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/events", true, http.HandlerFunc(handlers.GetAuthorEvents)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/replies", true, http.HandlerFunc(handlers.GetAuthorReplies)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/zaps", true, http.HandlerFunc(handlers.GetAuthorZaps)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/reactions", true, http.HandlerFunc(handlers.GetAuthorReactions)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/summary", true, http.HandlerFunc(handlers.GetAuthorAnalyticsSummary)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/topics", true, http.HandlerFunc(handlers.GetAuthorAnalyticsTopics)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/grouped-notes", true, http.HandlerFunc(handlers.GetAuthorAnalyticsGroupedNotes)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/media-mix", true, http.HandlerFunc(handlers.GetAuthorAnalyticsMediaMix)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/activity-windows", true, http.HandlerFunc(handlers.GetAuthorAnalyticsActivityWindows)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/posting-patterns", true, http.HandlerFunc(handlers.GetAuthorAnalyticsPostingPatterns)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/top-notes", true, http.HandlerFunc(handlers.GetAuthorAnalyticsTopNotes)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/performance-summary", true, http.HandlerFunc(handlers.GetAuthorAnalyticsPerformanceSummary)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/recycle-candidates", true, http.HandlerFunc(handlers.GetAuthorAnalyticsRecycleCandidates)),
		newPublicRoute("GET /api/v1/events/{id}/counts", true, http.HandlerFunc(handlers.GetEventCounts)),
		newPublicRoute("GET /api/v1/events/{id}/replies", true, cachedRead(handlers.GetEventReplies)),
		newPublicRoute("GET /api/v1/events/{id}/ancestors", true, cachedRead(handlers.GetEventAncestors)),
		newPublicRoute("GET /api/v1/events/{id}/actions", true, http.HandlerFunc(handlers.GetEventActions)),
		newPublicRoute("GET /api/v1/events/{id}/zaps", true, http.HandlerFunc(handlers.GetEventZaps)),
		newPublicRoute("GET /api/v1/threads/{eventId}", true, cachedRead(handlers.GetThread)),
		newPublicRoute("GET /api/v1/threads/{root_event_id}/summary", true, cachedRead(handlers.GetThreadSummary)),
		newPublicRoute("GET /api/v1/threads/{root_event_id}/activity", true, cachedRead(handlers.GetThreadActivity)),
		newPublicRoute("GET /api/v1/notes/{event_id}/summary", true, cachedRead(handlers.GetNoteSummary)),
		newPublicRoute("GET /api/v1/notes/{event_id}/related", true, http.HandlerFunc(handlers.GetNoteRelated)),
		newPublicRoute("GET /api/v1/relays/health", true, http.HandlerFunc(handlers.GetRelaysHealth)),
		newPublicRoute("GET /api/v1/relays/popular", true, http.HandlerFunc(handlers.GetPopularRelays)),
		newPublicRoute("GET /api/v1/relays/catalog", true, http.HandlerFunc(handlers.GetRelayCatalog)),
		newPublicRoute("GET /api/v1/relays/info", true, http.HandlerFunc(handlers.GetRelayInfo)),
		newPublicRoute("GET /api/v1/relays/probe-health", true, http.HandlerFunc(handlers.GetRelayProbeHealth)),
		newPublicRoute("GET /api/v1/contact-lists/{pubkey}", true, http.HandlerFunc(handlers.GetContactList)),
		newPublicRoute("GET /api/v1/relay-lists/{pubkey}", true, http.HandlerFunc(handlers.GetRelayList)),
		newPublicRoute("GET /api/v1/replaceable", true, http.HandlerFunc(handlers.GetReplaceable)),
		newPublicRoute("GET /api/v1/search", true, http.HandlerFunc(handlers.Search)),
		newPublicRoute("GET /api/v1/search/notes", true, http.HandlerFunc(handlers.SearchNotes)),
		newPublicRoute("GET /api/v1/search/profiles", true, http.HandlerFunc(handlers.SearchProfiles)),
		newPublicRoute("GET /api/v1/search/suggest", true, http.HandlerFunc(handlers.SearchSuggest)),
		newPublicRoute("GET /api/v1/discovery/notes/trending", true, http.HandlerFunc(handlers.GetTrendingNotes)),
		newPublicRoute("GET /api/v1/discovery/notes/bookmarked", true, http.HandlerFunc(handlers.GetBookmarkedNotes)),
		newPublicRoute("GET /api/v1/discovery/notes/zapped", true, http.HandlerFunc(handlers.GetMostZappedNotes)),
		newPublicRoute("GET /api/v1/discovery/long-form/trending", true, http.HandlerFunc(handlers.GetTrendingLongForm)),
		newPublicRoute("GET /api/v1/discovery/media/trending", true, http.HandlerFunc(handlers.GetTrendingMedia)),
		newPublicRoute("GET /api/v1/discovery/conversations/hot", true, http.HandlerFunc(handlers.GetHotConversations)),
		newPublicRoute("GET /api/v1/discovery/home", true, http.HandlerFunc(handlers.GetDiscoveryHome)),
		newPublicRoute("GET /api/v1/discovery/profiles/trending", true, http.HandlerFunc(handlers.GetTrendingProfiles)),
		newPublicRoute("GET /api/v1/discovery/profiles/rising", true, http.HandlerFunc(handlers.GetRisingProfiles)),
		newPublicRoute("GET /api/v1/discovery/profiles/followed", true, http.HandlerFunc(handlers.GetMostFollowedProfiles)),
		newPublicRoute("GET /api/v1/discovery/profiles/supported", true, http.HandlerFunc(handlers.GetMostSupportedProfiles)),
		newPublicRoute("GET /api/v1/discovery/profiles/{pubkey}/related", true, http.HandlerFunc(handlers.GetRelatedProfiles)),
		newPublicRoute("GET /api/v1/discovery/hashtags/trending", true, http.HandlerFunc(handlers.GetTrendingHashtags)),
		newPublicRoute("GET /api/v1/discovery/hashtags/{hashtag}", true, http.HandlerFunc(handlers.GetHashtagSummary)),
		newPublicRoute("GET /api/v1/discovery/hashtags/{hashtag}/notes", true, http.HandlerFunc(handlers.GetHashtagNotes)),
		newPublicRoute("GET /api/v1/discovery/hashtags/{hashtag}/related", true, http.HandlerFunc(handlers.GetRelatedHashtags)),
		newPublicRoute("GET /api/v1/discovery/domains/trending", true, http.HandlerFunc(handlers.GetTrendingDomains)),
		newPublicRoute("GET /api/v1/discovery/domains/{domain}", true, http.HandlerFunc(handlers.GetDomainSummary)),
		newPublicRoute("GET /api/v1/discovery/domains/{domain}/notes", true, http.HandlerFunc(handlers.GetDomainNotes)),
		newPublicRoute("GET /api/v1/discovery/stats/network", true, http.HandlerFunc(handlers.GetNetworkStats)),
		newPublicRoute("GET /api/v1/discovery/stats/content", true, http.HandlerFunc(handlers.GetContentStats)),
		newPublicRoute("GET /api/v1/discovery/stats/relays", true, http.HandlerFunc(handlers.GetRelayStats)),
		newPublicRoute("GET /api/v1/discovery/stats/series", true, http.HandlerFunc(handlers.GetDiscoveryStatsSeries)),
		newPublicRoute("GET /api/v1/discovery/stats/clients", true, http.HandlerFunc(handlers.GetClientStats)),
		newPublicRoute("GET /api/v1/discovery/stats/identity", true, http.HandlerFunc(handlers.GetIdentityStats)),
		newPublicRoute("GET /api/v1/discovery/stats/zaps", true, http.HandlerFunc(handlers.GetZapStats)),
		newPublicRoute("GET /api/v1/discovery/stats/accounts", true, http.HandlerFunc(handlers.GetAccountStats)),
		newPublicRoute("GET /api/v1/relays/propagation", true, http.HandlerFunc(handlers.GetRelayPropagation)),
		newPublicRoute("GET /api/v1/relays/overlap", true, http.HandlerFunc(handlers.GetRelayOverlap)),
		newPublicRoute("GET /api/v1/authors/{pubkey}/analytics/zaps", true, http.HandlerFunc(handlers.GetAuthorZapAnalytics)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}/identity", true, http.HandlerFunc(handlers.GetProfileIdentity)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}/external-identities", true, http.HandlerFunc(handlers.GetProfileExternalIdentities)),
		newPublicRoute("GET /api/v1/events/{id}/labels", true, cachedRead(handlers.GetEventLabels)),
		newPublicRoute("GET /api/v1/discovery/stats/labels", true, http.HandlerFunc(handlers.GetLabelStats)),
		newPublicRoute("GET /api/v1/discovery/stats/emoji", true, http.HandlerFunc(handlers.GetEmojiStats)),
		newPublicRoute("GET /api/v1/groups", true, http.HandlerFunc(handlers.GetGroups)),
		newPublicRoute("GET /api/v1/groups/{relayHost}/{groupId}", true, http.HandlerFunc(handlers.GetGroup)),
		newPublicRoute("GET /api/v1/groups/{relayHost}/{groupId}/messages", true, http.HandlerFunc(handlers.GetGroupMessages)),
		newPublicRoute("GET /api/v1/relays/{relayHost}/groups", true, http.HandlerFunc(handlers.GetRelayGroups)),
		newPublicRoute("GET /api/v1/discovery/stats/groups", true, http.HandlerFunc(handlers.GetGroupStats)),
		newPublicRoute("GET /api/v1/live/{address}/chat", true, http.HandlerFunc(handlers.GetLiveChat)),
		newPublicRoute("GET /api/v1/events/{id}/poll", true, http.HandlerFunc(handlers.GetEventPoll)),
		newPublicRoute("GET /api/v1/discovery/polls", true, http.HandlerFunc(handlers.GetPolls)),
		newPublicRoute("GET /api/v1/discovery/stats/polls", true, http.HandlerFunc(handlers.GetPollStats)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}/badges", true, http.HandlerFunc(handlers.GetProfileBadges)),
		newPublicRoute("GET /api/v1/badges/{address}", true, http.HandlerFunc(handlers.GetBadge)),
		newPublicRoute("GET /api/v1/discovery/stats/badges", true, http.HandlerFunc(handlers.GetBadgeStats)),
		newPublicRoute("GET /api/v1/events/{id}/goal", true, http.HandlerFunc(handlers.GetEventGoal)),
		newPublicRoute("GET /api/v1/discovery/goals", true, http.HandlerFunc(handlers.GetGoals)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}/status", true, http.HandlerFunc(handlers.GetProfileStatus)),
		newPublicRoute("GET /api/v1/discovery/curation-sets", true, http.HandlerFunc(handlers.GetCurationSets)),
		newPublicRoute("GET /api/v1/curation-sets/{address}", true, http.HandlerFunc(handlers.GetCurationSet)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}/interests", true, http.HandlerFunc(handlers.GetProfileInterests)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}/lists", true, http.HandlerFunc(handlers.GetProfileLists)),
		newPublicRoute("GET /api/v1/discovery/stats/conversations", true, http.HandlerFunc(handlers.GetConversationStats)),
		newPublicRoute("GET /api/v1/discovery/zaps/leaderboard", true, http.HandlerFunc(handlers.GetZapLeaderboard)),
		newPublicRoute("GET /api/v1/discovery/stats/zaps/heatmap", true, http.HandlerFunc(handlers.GetZapHeatmap)),
		newPublicRoute("GET /api/v1/hashtags/emerging", true, http.HandlerFunc(handlers.GetEmergingHashtags)),
		newPublicRoute("GET /api/v1/discovery/domains/emerging", true, http.HandlerFunc(handlers.GetEmergingDomains)),
		newPublicRoute("GET /api/v1/discovery/archive", true, http.HandlerFunc(handlers.ListDiscoveryArchive)),
		newPublicRoute("GET /api/v1/discovery/archive/{date}", true, http.HandlerFunc(handlers.GetDiscoveryArchive)),
		newPublicRoute("GET /api/v1/relays/coverage-changes", true, http.HandlerFunc(handlers.GetRelayCoverageChanges)),
		newPublicRoute("GET /api/v1/hashtags/{tag}/history", true, http.HandlerFunc(handlers.GetHashtagHistory)),
		newPublicRoute("GET /api/v1/discovery/stats/deletions", true, http.HandlerFunc(handlers.GetDeletionStats)),
		newPublicRoute("GET /api/v1/discovery/stats/moderation", true, http.HandlerFunc(handlers.GetModerationStats)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}/moderation", true, http.HandlerFunc(handlers.GetProfileModeration)),
		newPublicRoute("GET /api/v1/discovery/stats/spam", true, http.HandlerFunc(handlers.GetSpamStats)),
		newPublicRoute("GET /api/v1/discovery/stats/relay-lists", true, http.HandlerFunc(handlers.GetRelayListStats)),
		newPublicRoute("GET /api/v1/relays/{relayHost}/health/history", true, http.HandlerFunc(handlers.GetRelayHealthHistory)),
		newPublicRoute("GET /api/v1/discovery/stats/relay-software", true, http.HandlerFunc(handlers.GetRelaySoftwareStats)),
		newPublicRoute("GET /api/v1/discovery/stats/media-hosts", true, http.HandlerFunc(handlers.GetMediaHostStats)),
		newPublicRoute("GET /api/v1/discovery/stats/kinds", true, http.HandlerFunc(handlers.GetKindStats)),
		newPublicRoute("GET /api/v1/profiles/{pubkey}/labels", true, http.HandlerFunc(handlers.GetProfileLabels)),
		newPublicRoute("GET /api/v1/discovery/network/stats", true, http.HandlerFunc(handlers.GetNetworkStats)),
		newPublicRoute("GET /api/v1/discovery/content/stats", true, http.HandlerFunc(handlers.GetContentStats)),
		newPublicRoute("GET /api/v1/users/{pubkey}/bookmarks", true, http.HandlerFunc(handlers.GetBookmarks)),
		newPublicRoute("GET /api/v1/users/{pubkey}/highlights", true, http.HandlerFunc(handlers.GetHighlights)),
		newPublicRoute("GET /api/v1/users/{pubkey}/long-form", true, http.HandlerFunc(handlers.GetLongForm)),
		newPublicRoute("GET /api/v1/users/{pubkey}/media", true, http.HandlerFunc(handlers.GetMedia)),
		newPublicRoute("GET /api/v1/users/{pubkey}/live", true, http.HandlerFunc(handlers.GetUserLive)),
		newPublicRoute("GET /api/v1/users/{pubkey}/pins", true, http.HandlerFunc(handlers.GetPins)),
		newPublicRoute("GET /api/v1/live", true, http.HandlerFunc(handlers.GetLive)),
		newPublicRoute("GET /api/v1/events/{id}/comments", true, http.HandlerFunc(handlers.GetEventComments)),
		newPublicRoute("GET /api/v1/events/{id}/reports", true, cachedRead(handlers.GetEventReports)),
		newPublicRoute("GET /api/v1/users/{pubkey}/reports", true, http.HandlerFunc(handlers.GetUserReports)),
		newPublicRoute("GET /api/v1/users/{pubkey}/zaps", true, http.HandlerFunc(handlers.GetZaps)),
		newPublicRoute("GET /api/v1/users/{pubkey}/mentions", true, http.HandlerFunc(handlers.GetMentions)),
		newPublicRoute("GET /api/v1/users/{pubkey}/followers", true, http.HandlerFunc(handlers.GetFollowers)),
		newPublicRoute("GET /api/v1/users/{pubkey}/mutual-follows", true, http.HandlerFunc(handlers.GetMutualFollows)),
		newPublicRoute("GET /api/v1/users/{pubkey}/notifications", true, http.HandlerFunc(handlers.GetNotifications)),
		newPublicRoute("GET /api/v1/users/{pubkey}/mute-list", true, http.HandlerFunc(handlers.GetMuteList)),
		newPublicRoute("GET /api/v1/users/{pubkey}/muted-by", true, http.HandlerFunc(handlers.GetMutedBy)),
		newPublicRoute("GET /api/v1/users/{pubkey}/summary", true, http.HandlerFunc(handlers.GetProfilePublicSummary)),
		newPublicRoute("GET /api/v1/trust/scores/{pubkey}", false, http.HandlerFunc(handlers.GetTrustScore)),
		newPublicRoute("GET /api/v1/trust/scores", false, http.HandlerFunc(handlers.ListTopTrustScores)),
		newPublicRoute("GET /api/v1/accounts/{pubkey}/status", false, http.HandlerFunc(handlers.GetAccountStatus)),
		newPublicRoute("POST /api/v1/accounts/{pubkey}/hydrate", false, http.HandlerFunc(handlers.HydrateAccount)),

		newPublicRoute("GET /primal/v1/events/{id}", true, http.HandlerFunc(primalHandlers.GetEventByID)),
		newPublicRoute("POST /primal/v1/events/batch", true, http.HandlerFunc(primalHandlers.BatchGetEvents)),
		newPublicRoute("GET /primal/v1/profiles/{pubkey}", true, http.HandlerFunc(primalHandlers.GetProfileByPubkey)),
		newPublicRoute("POST /primal/v1/user_infos", true, http.HandlerFunc(primalHandlers.BatchGetUserInfos)),
		newPublicRoute("GET /primal/v1/threads/{eventId}", true, http.HandlerFunc(primalHandlers.GetThreadView)),
		newPublicRoute("GET /primal/v1/authors/{pubkey}/events", true, http.HandlerFunc(primalHandlers.GetAuthorEvents)),
		newPublicRoute("GET /primal/v1/authors/{pubkey}/replies", true, http.HandlerFunc(primalHandlers.GetAuthorReplies)),
		newPublicRoute("GET /primal/v1/events/{id}/actions", true, http.HandlerFunc(primalHandlers.GetEventActions)),
		newPublicRoute("GET /primal/v1/contact-lists/{pubkey}", true, http.HandlerFunc(primalHandlers.GetContactList)),
		newPublicRoute("GET /primal/v1/relay-lists/{pubkey}", true, http.HandlerFunc(primalHandlers.GetRelayList)),
		newPublicRoute("POST /primal/v1/dms/messages", true, http.HandlerFunc(primalHandlers.PostDirectMessages)),
		newPublicRoute("POST /primal/v1/dms/contacts", true, http.HandlerFunc(primalHandlers.PostDirectMessageContacts)),
		newPublicRoute("POST /primal/v1/dms/count", true, http.HandlerFunc(primalHandlers.PostDirectMessageCount)),
		newPublicRoute("POST /primal/v1/dms/count2", true, http.HandlerFunc(primalHandlers.PostDirectMessageCount2)),
		newPublicRoute("POST /primal/v1/dms/reset-count", true, http.HandlerFunc(primalHandlers.PostResetDirectMessageCount)),
		newPublicRoute("POST /primal/v1/dms/reset-counts", true, http.HandlerFunc(primalHandlers.PostResetDirectMessageCounts)),
		newPublicRoute("GET /primal/ws", true, http.HandlerFunc(primalWS.Handle)),
		newPublicRoute("GET /primal/ws/v1", true, v1Handler),

		newAdminRoute("GET /admin/v1/relays", true, http.HandlerFunc(adminHandlers.GetRelays)),
		newAdminRoute("GET /admin/v1/relays/suggestions", false, http.HandlerFunc(adminHandlers.GetRelaySuggestions)),
		newAdminRoute("GET /admin/v1/jobs", true, http.HandlerFunc(adminHandlers.GetJobs)),
		newAdminRoute("GET /admin/v1/invalid-events", true, http.HandlerFunc(adminHandlers.GetInvalidEvents)),
		newAdminRoute("GET /admin/v1/status/projections", true, http.HandlerFunc(adminHandlers.GetProjectionStatus)),
		newAdminRoute("GET /admin/v1/status/discovery", true, http.HandlerFunc(adminHandlers.GetDiscoveryStatus)),
		newAdminRoute("GET /admin/v1/status/search", true, http.HandlerFunc(adminHandlers.GetSearchStatus)),
		newAdminRoute("POST /admin/v1/search/meilisearch/sync", false, http.HandlerFunc(adminHandlers.TriggerMeilisearchSync)),
		newAdminRoute("GET /admin/v1/backfills/note-language", false, http.HandlerFunc(adminHandlers.GetLanguageBackfill)),
		newAdminRoute("POST /admin/v1/backfills/note-language", false, http.HandlerFunc(adminHandlers.StartLanguageBackfill)),
		newAdminRoute("GET /admin/v1/rebuilds", true, http.HandlerFunc(adminHandlers.GetRebuilds)),
		newAdminRoute("POST /admin/v1/rebuilds", true, http.HandlerFunc(adminHandlers.TriggerRebuild)),
		newAdminRoute("GET /admin/v1/storage", true, http.HandlerFunc(adminHandlers.GetStorage)),
		newAdminRoute("GET /admin/v1/storage/indexes", true, http.HandlerFunc(adminHandlers.GetStorageIndexes)),
		newAdminRoute("POST /admin/v1/accounts/{pubkey}/state", false, http.HandlerFunc(adminHandlers.SetAccountState)),
		newAdminRoute("POST /admin/v1/accounts/{pubkey}/hydrate", false, http.HandlerFunc(adminHandlers.HydrateAccount)),
		newAdminRoute("GET /admin/v1/system", true, http.HandlerFunc(adminHandlers.GetSystem)),
		newAdminRoute("GET /admin/v1/derivation-versions", true, http.HandlerFunc(adminHandlers.GetDerivationVersions)),
		newAdminRoute("GET /admin/v1/trust/runs", false, http.HandlerFunc(adminHandlers.GetTrustRuns)),
		newAdminRoute("GET /admin/v1/trust/runs/{runID}", false, http.HandlerFunc(adminHandlers.GetTrustRun)),
		newAdminRoute("POST /admin/v1/trust/runs", false, http.HandlerFunc(adminHandlers.TriggerTrustRun)),
		newAdminRoute("GET /admin/v1/trust/scores", false, http.HandlerFunc(adminHandlers.GetTopTrustScores)),
		newAdminRoute("GET /admin/v1/trust/interaction-rank-comparison", true, http.HandlerFunc(adminHandlers.GetTrustInteractionRankComparison)),

		newAdminRoute("GET /admin/v1/relay-registry", false, http.HandlerFunc(adminHandlers.GetRelayRegistry)),
		newAdminRoute("GET /admin/v1/relay-registry/desired", false, http.HandlerFunc(adminHandlers.GetRelayRegistryDesired)),
		newAdminRoute("POST /admin/v1/relay-registry/policy", false, http.HandlerFunc(adminHandlers.SetRelayRegistryPolicy)),
		newAdminRoute("GET /admin/v1/relay-registry/diagnostics", false, http.HandlerFunc(adminHandlers.GetRelayDiagnostics)),
		newAdminRoute("GET /admin/v1/relay-registry/admission-dry-run", false, http.HandlerFunc(adminHandlers.GetRelayAdmissionDryRun)),
	}
}

func registerDeclaredRoutes(publicMux, adminMux *http.ServeMux, defs []routeDefinition) {
	for _, route := range defs {
		if route.register == nil {
			panic("route registrar is required")
		}
		route.register(publicMux, adminMux, route.Pattern, route.Handler)
	}
}

func ContractOwnedRoutes() []routeDefinition {
	return buildRouteDefinitions(nil, api.Handlers{}, api_primal.Handlers{}, api_primal.WSGateway{}, api_primal.WSGateway{}, true, api.AdminHandlers{})
}
