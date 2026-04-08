package api_primal

import (
	"context"
	"errors"
	"strings"
)

type cacheCallHandler func(WSGateway, context.Context, map[string]any) ([]any, error)

type cacheCallHandlerFamily string

const (
	cacheCallFamilyDispatch cacheCallHandlerFamily = "dispatch"
	cacheCallFamilyResolve  cacheCallHandlerFamily = "resolve"
	cacheCallFamilyBuild    cacheCallHandlerFamily = "build"
)

type cacheCallRoute struct {
	family  cacheCallHandlerFamily
	handler cacheCallHandler
}

var cacheCallRoutes = map[string]cacheCallRoute{
	"events":                          {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchEvents},
	"user_profile":                    {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchUserProfile},
	"user_infos":                      {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchUserInfos},
	"thread_view":                     {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchThreadView},
	"feed":                            {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchFeed},
	"author_replies":                  {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchAuthorReplies},
	"event_actions":                   {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchEventActions},
	"contact_list":                    {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchContactList},
	"relay_list":                      {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchRelayList},
	"search":                          {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchSearch},
	"user_zaps":                       {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchUserZaps},
	"user_zaps_by_satszapped":         {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchUserZapsBySats},
	"event_zaps_by_satszapped":        {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchEventZapsBySats},
	"is_user_following":               {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchIsUserFollowing},
	"mutual_follows":                  {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchMutualFollows},
	"get_directmsg_contacts":          {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchDirectMessageContacts},
	"get_bookmarks":                   {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchBookmarks},
	"get_highlights":                  {family: cacheCallFamilyResolve, handler: WSGateway.resolveHighlightsResponse},
	"long_form_content_feed":          {family: cacheCallFamilyResolve, handler: WSGateway.resolveLongFormContentFeed},
	"long_form_content_thread_view":   {family: cacheCallFamilyResolve, handler: WSGateway.resolveLongFormContentThreadView},
	"get_directmsgs":                  {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchDirectMessages},
	"directmsg_count":                 {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchDirectMessageCount},
	"directmsg_count_2":               {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchDirectMessageCount2},
	"reset_directmsg_count":           {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchResetDirectMessageCount},
	"reset_directmsg_counts":          {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchResetDirectMessageCounts},
	"user_mentions":                   {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchUserMentions},
	"user_followers":                  {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchUserFollowers},
	"mutelist":                        {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchMutelist},
	"mutelists":                       {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchMutelists},
	"allowlist":                       {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchAllowlist},
	"is_hidden_by_content_moderation": {family: cacheCallFamilyBuild, handler: WSGateway.buildHiddenByContentModerationResponse},
	"search_filterlist":               {family: cacheCallFamilyBuild, handler: WSGateway.buildSearchFilterlistResponse},
	"parameterized_replaceable_list":  {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchParameterizedReplaceableList},
	"parametrized_replaceable_event":  {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchParametrizedReplaceableEvent},
	"parametrized_replaceable_events": {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchParametrizedReplaceableEvents},
	"network_stats":                   {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchNetworkStats},
	"net_stats":                       {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchNetworkStats},
	"nostr_stats":                     {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchNetworkStats},
	"server_name":                     {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchServerName},
	"get_recommended_reads":           {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchRecommendedReads},
	"get_reads_topics":                {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchReadsTopics},
	"get_featured_authors":            {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchFeaturedAuthors},
	"creator_paid_tiers":              {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchCreatorPaidTiers},
	"user_of_ln_address":              {family: cacheCallFamilyDispatch, handler: WSGateway.cacheDispatchUserOfLNAddress},
}

func (g WSGateway) dispatchCacheCall(ctx context.Context, reqName string, kwargs map[string]any) ([]any, error) {
	route, ok := cacheCallRoutes[strings.ToLower(strings.TrimSpace(reqName))]
	if !ok {
		return nil, errors.New("unsupported")
	}
	return route.handler(g, ctx, kwargs)
}
