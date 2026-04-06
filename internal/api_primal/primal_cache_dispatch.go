package api_primal

import (
	"context"
	"errors"
	"strings"
)

func (g WSGateway) dispatchCacheCall(ctx context.Context, reqName string, kwargs map[string]any) ([]any, error) {
	switch strings.ToLower(strings.TrimSpace(reqName)) {
	case "events":
		return g.cacheDispatchEvents(ctx, kwargs)
	case "user_profile":
		return g.cacheDispatchUserProfile(ctx, kwargs)
	case "user_infos":
		return g.cacheDispatchUserInfos(ctx, kwargs)
	case "thread_view":
		return g.cacheDispatchThreadView(ctx, kwargs)
	case "feed":
		return g.cacheDispatchFeed(ctx, kwargs)
	case "author_replies":
		return g.cacheDispatchAuthorReplies(ctx, kwargs)
	case "event_actions":
		return g.cacheDispatchEventActions(ctx, kwargs)
	case "contact_list":
		return g.cacheDispatchContactList(ctx, kwargs)
	case "relay_list":
		return g.cacheDispatchRelayList(ctx, kwargs)
	case "search":
		return g.cacheDispatchSearch(ctx, kwargs)
	case "user_zaps":
		return g.cacheDispatchUserZaps(ctx, kwargs)
	case "user_zaps_by_satszapped":
		return g.cacheDispatchUserZapsBySats(ctx, kwargs)
	case "event_zaps_by_satszapped":
		return g.cacheDispatchEventZapsBySats(ctx, kwargs)
	case "is_user_following":
		return g.cacheDispatchIsUserFollowing(ctx, kwargs)
	case "mutual_follows":
		return g.cacheDispatchMutualFollows(ctx, kwargs)
	case "get_directmsg_contacts":
		return g.cacheDispatchDirectMessageContacts(ctx, kwargs)
	case "get_bookmarks":
		return g.cacheDispatchBookmarks(ctx, kwargs)
	case "get_highlights":
		return g.resolveHighlightsResponse(ctx, kwargs)
	case "long_form_content_feed":
		return g.resolveLongFormContentFeed(ctx, kwargs)
	case "long_form_content_thread_view":
		return g.resolveLongFormContentThreadView(ctx, kwargs)
	case "get_directmsgs":
		return g.cacheDispatchDirectMessages(ctx, kwargs)
	case "directmsg_count":
		return g.cacheDispatchDirectMessageCount(ctx, kwargs)
	case "directmsg_count_2":
		return g.cacheDispatchDirectMessageCount2(ctx, kwargs)
	case "reset_directmsg_count":
		return g.cacheDispatchResetDirectMessageCount(ctx, kwargs)
	case "reset_directmsg_counts":
		return g.cacheDispatchResetDirectMessageCounts(ctx, kwargs)
	case "user_mentions":
		return g.cacheDispatchUserMentions(ctx, kwargs)
	case "user_followers":
		return g.cacheDispatchUserFollowers(ctx, kwargs)
	case "mutelist":
		return g.cacheDispatchMutelist(ctx, kwargs)
	case "mutelists":
		return g.cacheDispatchMutelists(ctx, kwargs)
	case "allowlist":
		return g.cacheDispatchAllowlist(ctx, kwargs)
	case "is_hidden_by_content_moderation":
		return g.buildHiddenByContentModerationResponse(ctx, kwargs)
	case "search_filterlist":
		return g.buildSearchFilterlistResponse(ctx, kwargs)
	case "parameterized_replaceable_list":
		return g.cacheDispatchParameterizedReplaceableList(ctx, kwargs)
	case "parametrized_replaceable_event":
		return g.cacheDispatchParametrizedReplaceableEvent(ctx, kwargs)
	case "parametrized_replaceable_events":
		return g.cacheDispatchParametrizedReplaceableEvents(ctx, kwargs)
	case "network_stats", "net_stats", "nostr_stats":
		return g.cacheDispatchNetworkStats(ctx, kwargs)
	case "server_name":
		return g.cacheDispatchServerName(ctx, kwargs)
	case "get_recommended_reads":
		return g.cacheDispatchRecommendedReads(ctx, kwargs)
	case "get_reads_topics":
		return g.cacheDispatchReadsTopics(ctx, kwargs)
	case "get_featured_authors":
		return g.cacheDispatchFeaturedAuthors(ctx, kwargs)
	case "creator_paid_tiers":
		return g.cacheDispatchCreatorPaidTiers(ctx, kwargs)
	case "user_of_ln_address":
		return g.cacheDispatchUserOfLNAddress(ctx, kwargs)
	default:
		return nil, errors.New("unsupported")
	}
}
