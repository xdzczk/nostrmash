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
	noteAuthorPubkeys := make([]string, 0, len(notes))
	for _, note := range notes {
		noteAuthorPubkeys = append(noteAuthorPubkeys, note.AuthorPubkey)
	}
	noteAuthorIdentities, err := h.resolveProfileIdentities(r.Context(), noteAuthorPubkeys)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := buildDiscoveryNoteItems(notes, noteAuthorIdentities)
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

func (h Handlers) GetTrendingLongForm(w http.ResponseWriter, r *http.Request) {
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
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "trending_long_form", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	articles, err := h.service.GetTrendingLongForm(r.Context(), window, limit, offset)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trending long-form is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	authorPubkeys := make([]string, 0, len(articles))
	for _, article := range articles {
		authorPubkeys = append(authorPubkeys, article.AuthorPubkey)
	}
	authorIdentities, err := h.resolveProfileIdentities(r.Context(), authorPubkeys)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := buildDiscoveryNoteItems(articles, authorIdentities)
	payloadResponse := map[string]any{
		"surface":     "trending_long_form",
		"window":      windowLabel,
		"articles":    payload,
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
		item := map[string]any{
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
		}
		item["preview"] = buildNotePreviewPayload(conversation.RootEventID, conversation.Content)
		items = append(items, item)
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
	identityPubkeys := make([]string, 0, len(notes)+len(trendingProfiles)+len(risingProfiles))
	for _, note := range notes {
		identityPubkeys = append(identityPubkeys, note.AuthorPubkey)
	}
	for _, profile := range trendingProfiles {
		identityPubkeys = append(identityPubkeys, profile.Pubkey)
	}
	for _, profile := range risingProfiles {
		identityPubkeys = append(identityPubkeys, profile.Pubkey)
	}
	profileIdentities, err := h.resolveProfileIdentities(r.Context(), identityPubkeys)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	noteItems := buildDiscoveryNoteItems(notes, profileIdentities)
	hashtagItems := make([]map[string]any, 0, len(hashtags))
	for _, hashtag := range hashtags {
		hashtagItems = append(hashtagItems, map[string]any{
			"hashtag":        hashtag.Hashtag,
			"event_count":    hashtag.EventCount,
			"unique_authors": hashtag.UniqueAuthors,
		})
	}
	trendingProfileItems := buildDiscoveryProfileItems(trendingProfiles, profileIdentities)
	risingProfileItems := buildDiscoveryProfileItems(risingProfiles, profileIdentities)
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
