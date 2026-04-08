package api_primal

import (
	"context"
	"errors"
	"strings"
)

type cacheCallHandler func(WSGateway, context.Context, map[string]any) ([]any, error)

var cacheCallHandlers = map[string]cacheCallHandler{
	"events":                          WSGateway.cacheDispatchEvents,
	"user_profile":                    WSGateway.cacheDispatchUserProfile,
	"user_infos":                      WSGateway.cacheDispatchUserInfos,
	"thread_view":                     WSGateway.cacheDispatchThreadView,
	"feed":                            WSGateway.cacheDispatchFeed,
	"author_replies":                  WSGateway.cacheDispatchAuthorReplies,
	"event_actions":                   WSGateway.cacheDispatchEventActions,
	"contact_list":                    WSGateway.cacheDispatchContactList,
	"relay_list":                      WSGateway.cacheDispatchRelayList,
	"search":                          WSGateway.cacheDispatchSearch,
	"user_zaps":                       WSGateway.cacheDispatchUserZaps,
	"user_zaps_by_satszapped":         WSGateway.cacheDispatchUserZapsBySats,
	"event_zaps_by_satszapped":        WSGateway.cacheDispatchEventZapsBySats,
	"is_user_following":               WSGateway.cacheDispatchIsUserFollowing,
	"mutual_follows":                  WSGateway.cacheDispatchMutualFollows,
	"get_directmsg_contacts":          WSGateway.cacheDispatchDirectMessageContacts,
	"get_bookmarks":                   WSGateway.cacheDispatchBookmarks,
	"get_highlights":                  WSGateway.resolveHighlightsResponse,
	"long_form_content_feed":          WSGateway.resolveLongFormContentFeed,
	"long_form_content_thread_view":   WSGateway.resolveLongFormContentThreadView,
	"get_directmsgs":                  WSGateway.cacheDispatchDirectMessages,
	"directmsg_count":                 WSGateway.cacheDispatchDirectMessageCount,
	"directmsg_count_2":               WSGateway.cacheDispatchDirectMessageCount2,
	"reset_directmsg_count":           WSGateway.cacheDispatchResetDirectMessageCount,
	"reset_directmsg_counts":          WSGateway.cacheDispatchResetDirectMessageCounts,
	"user_mentions":                   WSGateway.cacheDispatchUserMentions,
	"user_followers":                  WSGateway.cacheDispatchUserFollowers,
	"mutelist":                        WSGateway.cacheDispatchMutelist,
	"mutelists":                       WSGateway.cacheDispatchMutelists,
	"allowlist":                       WSGateway.cacheDispatchAllowlist,
	"is_hidden_by_content_moderation": WSGateway.buildHiddenByContentModerationResponse,
	"search_filterlist":               WSGateway.buildSearchFilterlistResponse,
	"parameterized_replaceable_list":  WSGateway.cacheDispatchParameterizedReplaceableList,
	"parametrized_replaceable_event":  WSGateway.cacheDispatchParametrizedReplaceableEvent,
	"parametrized_replaceable_events": WSGateway.cacheDispatchParametrizedReplaceableEvents,
	"network_stats":                   WSGateway.cacheDispatchNetworkStats,
	"net_stats":                       WSGateway.cacheDispatchNetworkStats,
	"nostr_stats":                     WSGateway.cacheDispatchNetworkStats,
	"server_name":                     WSGateway.cacheDispatchServerName,
	"get_recommended_reads":           WSGateway.cacheDispatchRecommendedReads,
	"get_reads_topics":                WSGateway.cacheDispatchReadsTopics,
	"get_featured_authors":            WSGateway.cacheDispatchFeaturedAuthors,
	"creator_paid_tiers":              WSGateway.cacheDispatchCreatorPaidTiers,
	"user_of_ln_address":              WSGateway.cacheDispatchUserOfLNAddress,
}

func (g WSGateway) dispatchCacheCall(ctx context.Context, reqName string, kwargs map[string]any) ([]any, error) {
	handler, ok := cacheCallHandlers[strings.ToLower(strings.TrimSpace(reqName))]
	if !ok {
		return nil, errors.New("unsupported")
	}
	return handler(g, ctx, kwargs)
}
