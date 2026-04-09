package api

import (
	"net/http"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetTrendingNotes(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "trending_notes", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	notes, err := h.service.GetTrendingNotes(r.Context(), window, limit, offset)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trending notes are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := make([]map[string]any, 0, len(notes))
	for _, note := range notes {
		payload = append(payload, map[string]any{
			"event_id":       note.EventID,
			"author_pubkey":  note.AuthorPubkey,
			"created_at":     note.CreatedAt,
			"content":        note.Content,
			"language":       note.Language,
			"reply_count":    note.ReplyCount,
			"repost_count":   note.RepostCount,
			"reaction_count": note.ReactionCount,
			"zap_count":      note.ZapCount,
			"zap_msats":      note.ZapMSats,
			"score":          note.Score,
		})
	}
	payloadResponse := map[string]any{
		"surface":     "trending",
		"window":      windowLabel,
		"notes":       payload,
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payloadResponse)
	h.cachePublicPayload(cachePolicy, payloadResponse)
	writeJSON(w, http.StatusOK, payloadResponse)
}

func (h Handlers) GetHotConversations(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "hot_conversations", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	conversations, err := h.service.GetHotConversations(r.Context(), window, limit, offset)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "hot conversations are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	items := make([]map[string]any, 0, len(conversations))
	for _, conversation := range conversations {
		items = append(items, map[string]any{
			"root_event_id":     conversation.RootEventID,
			"author_pubkey":     conversation.AuthorPubkey,
			"created_at":        conversation.CreatedAt,
			"content":           conversation.Content,
			"reply_count":       conversation.ReplyCount,
			"participant_count": conversation.ParticipantCount,
			"last_activity_at":  conversation.LastActivityAt,
			"activity": map[string]any{
				"replies_24h": conversation.Replies24h,
				"replies_7d":  conversation.Replies7d,
			},
			"velocity_score": conversation.VelocityScore,
		})
	}
	payloadResponse := map[string]any{
		"surface":       "hot_conversations",
		"window":        windowLabel,
		"conversations": items,
		"consistency":   "eventual",
	}
	h.addDiscoveryTrustMetadata(payloadResponse)
	h.cachePublicPayload(cachePolicy, payloadResponse)
	writeJSON(w, http.StatusOK, payloadResponse)
}

func (h Handlers) GetDiscoveryHome(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	notesLimit, err := parseBoundedPositiveInt(r, "notes_limit", 10, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	hashtagsLimit, err := parseBoundedPositiveInt(r, "hashtags_limit", 10, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	profilesLimit, err := parseBoundedPositiveInt(r, "profiles_limit", 10, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	hashtagStatLimit, err := parseBoundedPositiveInt(r, "hashtag_stat_limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyBundle, "discovery_home", map[string]any{
		"window":             windowLabel,
		"notes_limit":        notesLimit,
		"hashtags_limit":     hashtagsLimit,
		"profiles_limit":     profilesLimit,
		"hashtag_stat_limit": hashtagStatLimit,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	notes, err := h.service.GetTrendingNotes(r.Context(), window, notesLimit, 0)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "homepage discovery bundle is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	hashtags, err := h.service.GetTrendingHashtags(r.Context(), window, hashtagsLimit, 0)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "homepage discovery bundle is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	trendingProfiles, err := h.service.GetTrendingProfiles(r.Context(), window, profilesLimit, 0)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "homepage discovery bundle is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	risingProfiles, err := h.service.GetRisingProfiles(r.Context(), window, profilesLimit, 0)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "homepage discovery bundle is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	networkStats, err := h.service.GetPublicDiscoveryNetworkStats(r.Context(), hashtagStatLimit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "homepage discovery bundle is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	noteItems := make([]map[string]any, 0, len(notes))
	for _, note := range notes {
		noteItems = append(noteItems, map[string]any{
			"event_id":       note.EventID,
			"author_pubkey":  note.AuthorPubkey,
			"created_at":     note.CreatedAt,
			"content":        note.Content,
			"language":       note.Language,
			"reply_count":    note.ReplyCount,
			"repost_count":   note.RepostCount,
			"reaction_count": note.ReactionCount,
			"zap_count":      note.ZapCount,
			"zap_msats":      note.ZapMSats,
			"score":          note.Score,
		})
	}
	hashtagItems := make([]map[string]any, 0, len(hashtags))
	for _, hashtag := range hashtags {
		hashtagItems = append(hashtagItems, map[string]any{
			"hashtag":        hashtag.Hashtag,
			"event_count":    hashtag.EventCount,
			"unique_authors": hashtag.UniqueAuthors,
		})
	}
	trendingProfileItems := make([]map[string]any, 0, len(trendingProfiles))
	for _, profile := range trendingProfiles {
		trendingProfileItems = append(trendingProfileItems, map[string]any{
			"pubkey":                     profile.Pubkey,
			"score":                      profile.Score,
			"recent_post_count":          profile.RecentPostCount,
			"recent_reply_count":         profile.RecentReplyCount,
			"recent_engagement_received": profile.RecentEngagementReceived,
			"recent_zap_volume_msats":    profile.RecentZapVolumeMSats,
			"recent_active_days":         profile.RecentActiveDays,
			"recent_activity_at":         profile.RecentActivityAt,
		})
	}
	risingProfileItems := make([]map[string]any, 0, len(risingProfiles))
	for _, profile := range risingProfiles {
		risingProfileItems = append(risingProfileItems, map[string]any{
			"pubkey":                     profile.Pubkey,
			"score":                      profile.Score,
			"recent_post_count":          profile.RecentPostCount,
			"recent_reply_count":         profile.RecentReplyCount,
			"recent_engagement_received": profile.RecentEngagementReceived,
			"recent_zap_volume_msats":    profile.RecentZapVolumeMSats,
			"recent_active_days":         profile.RecentActiveDays,
			"recent_activity_at":         profile.RecentActivityAt,
		})
	}
	network := map[string]any{
		"totals": map[string]any{
			"events_ingested":    networkStats.EventsIngested,
			"projected_profiles": networkStats.ProjectedProfiles,
		},
		"activity": map[string]any{
			"active_authors": networkStats.ActiveAuthors,
			"note_volume":    networkStats.NoteVolume,
		},
		"relays": buildRelayStatsPayload(networkStats),
	}
	if networkStats.TopHashtags != nil {
		network["top_hashtags"] = networkStats.TopHashtags
	}
	if len(networkStats.TopLanguages24h) > 0 || len(networkStats.TopLanguages7d) > 0 {
		network["top_languages"] = map[string]any{
			"24h": networkStats.TopLanguages24h,
			"7d":  networkStats.TopLanguages7d,
		}
	}

	payload := map[string]any{
		"surface": "home",
		"window":  windowLabel,
		"sections": map[string]any{
			"trending_notes":    noteItems,
			"trending_hashtags": hashtagItems,
			"profiles": map[string]any{
				"trending": trendingProfileItems,
				"rising":   risingProfileItems,
			},
			"network_summary": network,
		},
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payload)
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}
