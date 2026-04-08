package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

var errInvalidTrendingWindow = errors.New("window must be one of: 24h, 7d")
var errInvalidHashtagNotesWindow = errors.New("window must be one of: 24h, 7d, 30d, all")
var errInvalidHashtagNotesSort = errors.New("sort must be one of: latest, top")
var errInvalidDomainNotesWindow = errors.New("window must be one of: 24h, 7d, 30d, all")
var errInvalidDomainNotesSort = errors.New("sort must be one of: latest, top")

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

func (h Handlers) GetTrendingProfiles(w http.ResponseWriter, r *http.Request) {
	h.writeDiscoveryProfiles(w, r, "trending")
}

func (h Handlers) GetRisingProfiles(w http.ResponseWriter, r *http.Request) {
	h.writeDiscoveryProfiles(w, r, "rising")
}

func (h Handlers) GetTrendingHashtags(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingHashtagWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "trending_hashtags", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	topics, err := h.service.GetTrendingHashtags(r.Context(), window, limit, offset)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trending hashtags are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	hashtags := make([]map[string]any, 0, len(topics))
	for _, topic := range topics {
		hashtags = append(hashtags, map[string]any{
			"hashtag":        topic.Hashtag,
			"event_count":    topic.EventCount,
			"unique_authors": topic.UniqueAuthors,
		})
	}
	payloadResponse := map[string]any{
		"surface":     "trending",
		"hashtags":    hashtags,
		"window":      windowLabel,
		"consistency": "eventual",
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
		"relays": map[string]any{
			"total": networkStats.Relays,
		},
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

func (h Handlers) GetHashtagSummary(w http.ResponseWriter, r *http.Request) {
	rawHashtag := r.PathValue("hashtag")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "hashtag_summary", map[string]any{
		"hashtag": normalizeCacheHashtag(rawHashtag),
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	summary, err := h.service.GetHashtagSummary(r.Context(), rawHashtag)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "hashtag not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "hashtag summary is not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidHashtag) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "hashtag is invalid")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := map[string]any{
		"hashtag":         summary.Hashtag,
		"latest_event_at": summary.LatestEventAt,
		"activity": map[string]any{
			"24h": map[string]any{
				"event_count":    summary.Activity.Last24h.EventCount,
				"unique_authors": summary.Activity.Last24h.UniqueAuthors,
			},
			"7d": map[string]any{
				"event_count":    summary.Activity.Last7d.EventCount,
				"unique_authors": summary.Activity.Last7d.UniqueAuthors,
			},
			"30d": map[string]any{
				"event_count":    summary.Activity.Last30d.EventCount,
				"unique_authors": summary.Activity.Last30d.UniqueAuthors,
			},
			"all": map[string]any{
				"event_count":    summary.Activity.All.EventCount,
				"unique_authors": summary.Activity.All.UniqueAuthors,
			},
		},
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payload)
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetHashtagNotes(w http.ResponseWriter, r *http.Request) {
	sort, err := parseHashtagNotesSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window, err := parseHashtagNotesWindow(r)
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
	rawHashtag := r.PathValue("hashtag")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "hashtag_notes", map[string]any{
		"hashtag": normalizeCacheHashtag(rawHashtag),
		"sort":    sort,
		"window":  window,
		"limit":   limit,
		"offset":  offset,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	notes, err := h.service.GetHashtagNotes(r.Context(), rawHashtag, sort, window, limit, offset)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "hashtag not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "hashtag notes are not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidHashtag) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "hashtag is invalid")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payloadNotes := make([]map[string]any, 0, len(notes))
	for _, note := range notes {
		payloadNotes = append(payloadNotes, map[string]any{
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
	payload := map[string]any{
		"hashtag":     rawHashtag,
		"sort":        sort,
		"window":      window,
		"notes":       payloadNotes,
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payload)
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetRelatedHashtags(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	rawHashtag := r.PathValue("hashtag")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "related_hashtags", map[string]any{
		"hashtag": normalizeCacheHashtag(rawHashtag),
		"limit":   limit,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	related, err := h.service.GetRelatedHashtags(r.Context(), rawHashtag, limit)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "hashtag not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "related hashtags are not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidHashtag) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "hashtag is invalid")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	items := make([]map[string]any, 0, len(related))
	for _, row := range related {
		items = append(items, map[string]any{
			"hashtag":               row.Hashtag,
			"co_occurrence_count":   row.CoOccurrenceCount,
			"co_occurrence_authors": row.CoOccurrenceAuthors,
		})
	}
	payload := map[string]any{
		"hashtag":     rawHashtag,
		"related":     items,
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payload)
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetTrendingDomains(w http.ResponseWriter, r *http.Request) {
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
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "trending_domains", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	rows, err := h.service.GetTrendingDomains(r.Context(), window, limit, offset)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trending domains are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	domains := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		domains = append(domains, map[string]any{
			"domain":          row.Domain,
			"latest_event_at": row.LatestEventAt,
			"link_count":      row.Activity.Last7d.LinkCount,
			"note_count":      row.Activity.Last7d.NoteCount,
			"unique_authors":  row.Activity.Last7d.UniqueAuthors,
			"trend_windows": map[string]any{
				"24h": map[string]any{
					"link_count":     row.Activity.Last24h.LinkCount,
					"note_count":     row.Activity.Last24h.NoteCount,
					"unique_authors": row.Activity.Last24h.UniqueAuthors,
				},
				"7d": map[string]any{
					"link_count":     row.Activity.Last7d.LinkCount,
					"note_count":     row.Activity.Last7d.NoteCount,
					"unique_authors": row.Activity.Last7d.UniqueAuthors,
				},
			},
		})
	}
	payload := map[string]any{
		"surface":     "trending",
		"window":      windowLabel,
		"domains":     domains,
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payload)
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetDomainSummary(w http.ResponseWriter, r *http.Request) {
	rawDomain := r.PathValue("domain")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "domain_summary", map[string]any{
		"domain": strings.ToLower(strings.TrimSpace(rawDomain)),
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	summary, err := h.service.GetDomainSummary(r.Context(), rawDomain)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "domain not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "domain summary is not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidDomain) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "domain is invalid")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	recentNotes := make([]map[string]any, 0, len(summary.RecentNotes))
	for _, note := range summary.RecentNotes {
		recentNotes = append(recentNotes, map[string]any{
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
	topNotes := make([]map[string]any, 0, len(summary.TopNotes))
	for _, note := range summary.TopNotes {
		topNotes = append(topNotes, map[string]any{
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
	payload := map[string]any{
		"domain":          summary.Domain,
		"latest_event_at": summary.LatestEventAt,
		"activity": map[string]any{
			"24h": map[string]any{
				"link_count":     summary.Activity.Last24h.LinkCount,
				"note_count":     summary.Activity.Last24h.NoteCount,
				"unique_authors": summary.Activity.Last24h.UniqueAuthors,
			},
			"7d": map[string]any{
				"link_count":     summary.Activity.Last7d.LinkCount,
				"note_count":     summary.Activity.Last7d.NoteCount,
				"unique_authors": summary.Activity.Last7d.UniqueAuthors,
			},
			"30d": map[string]any{
				"link_count":     summary.Activity.Last30d.LinkCount,
				"note_count":     summary.Activity.Last30d.NoteCount,
				"unique_authors": summary.Activity.Last30d.UniqueAuthors,
			},
			"all": map[string]any{
				"link_count":     summary.Activity.All.LinkCount,
				"note_count":     summary.Activity.All.NoteCount,
				"unique_authors": summary.Activity.All.UniqueAuthors,
			},
		},
		"notes": map[string]any{
			"recent": recentNotes,
			"top":    topNotes,
		},
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payload)
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetDomainNotes(w http.ResponseWriter, r *http.Request) {
	sort, err := parseDomainNotesSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window, err := parseDomainNotesWindow(r)
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
	rawDomain := r.PathValue("domain")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "domain_notes", map[string]any{
		"domain": strings.ToLower(strings.TrimSpace(rawDomain)),
		"sort":   sort,
		"window": window,
		"limit":  limit,
		"offset": offset,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	notes, err := h.service.GetDomainNotes(r.Context(), rawDomain, sort, window, limit, offset)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "domain not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "domain notes are not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidDomain) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "domain is invalid")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payloadNotes := make([]map[string]any, 0, len(notes))
	for _, note := range notes {
		payloadNotes = append(payloadNotes, map[string]any{
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
	payload := map[string]any{
		"domain":      rawDomain,
		"sort":        sort,
		"window":      window,
		"notes":       payloadNotes,
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payload)
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}

func parseTrendingHashtagWindow(r *http.Request) (time.Duration, string, error) {
	return parseTrendingWindow(r)
}

func parseHashtagNotesWindow(r *http.Request) (string, error) {
	raw := r.URL.Query().Get("window")
	switch raw {
	case "", "24h":
		return "24h", nil
	case "7d":
		return "7d", nil
	case "30d":
		return "30d", nil
	case "all":
		return "all", nil
	default:
		return "", errInvalidHashtagNotesWindow
	}
}

func parseHashtagNotesSort(r *http.Request) (string, error) {
	raw := r.URL.Query().Get("sort")
	switch raw {
	case "", "latest":
		return "latest", nil
	case "top":
		return "top", nil
	default:
		return "", errInvalidHashtagNotesSort
	}
}

func parseDomainNotesWindow(r *http.Request) (string, error) {
	raw := r.URL.Query().Get("window")
	switch raw {
	case "", "24h":
		return "24h", nil
	case "7d":
		return "7d", nil
	case "30d":
		return "30d", nil
	case "all":
		return "all", nil
	default:
		return "", errInvalidDomainNotesWindow
	}
}

func parseDomainNotesSort(r *http.Request) (string, error) {
	raw := r.URL.Query().Get("sort")
	switch raw {
	case "", "latest":
		return "latest", nil
	case "top":
		return "top", nil
	default:
		return "", errInvalidDomainNotesSort
	}
}

func parseTrendingWindow(r *http.Request) (time.Duration, string, error) {
	raw := r.URL.Query().Get("window")
	switch raw {
	case "", "24h":
		return 24 * time.Hour, "24h", nil
	case "7d":
		return 7 * 24 * time.Hour, "7d", nil
	default:
		return 0, "", errInvalidTrendingWindow
	}
}

func (h Handlers) GetNetworkStats(w http.ResponseWriter, r *http.Request) {
	hashtagLimit, err := parseBoundedPositiveInt(r, "hashtag_limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyStats, "network_stats", map[string]any{
		"hashtag_limit": hashtagLimit,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	stats, err := h.service.GetPublicDiscoveryNetworkStats(r.Context(), hashtagLimit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "network stats are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	network := map[string]any{
		"totals": map[string]any{
			"events_ingested":    stats.EventsIngested,
			"projected_profiles": stats.ProjectedProfiles,
		},
		"activity": map[string]any{
			"active_authors": stats.ActiveAuthors,
			"note_volume":    stats.NoteVolume,
		},
		"relays": map[string]any{
			"total": stats.Relays,
		},
	}
	if stats.TopHashtags != nil {
		network["top_hashtags"] = stats.TopHashtags
	}
	if len(stats.TopLanguages24h) > 0 || len(stats.TopLanguages7d) > 0 {
		network["top_languages"] = map[string]any{
			"24h": stats.TopLanguages24h,
			"7d":  stats.TopLanguages7d,
		}
	}
	payload := map[string]any{
		"network":     network,
		"consistency": "eventual",
	}
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetContentStats(w http.ResponseWriter, r *http.Request) {
	hashtagLimit, err := parseBoundedPositiveInt(r, "hashtag_limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyStats, "content_stats", map[string]any{
		"hashtag_limit": hashtagLimit,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	stats, err := h.service.GetPublicDiscoveryNetworkStats(r.Context(), hashtagLimit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "content stats are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	content := map[string]any{
		"totals": map[string]any{
			"events_ingested":    stats.EventsIngested,
			"projected_profiles": stats.ProjectedProfiles,
		},
		"note_volume": stats.NoteVolume,
	}
	if stats.TopHashtags != nil {
		content["top_hashtags"] = stats.TopHashtags
	}
	if len(stats.TopLanguages24h) > 0 || len(stats.TopLanguages7d) > 0 {
		content["top_languages"] = map[string]any{
			"24h": stats.TopLanguages24h,
			"7d":  stats.TopLanguages7d,
		}
	}
	payload := map[string]any{
		"content":     content,
		"consistency": "eventual",
	}
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetRelayStats(w http.ResponseWriter, r *http.Request) {
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyStats, "relay_stats", nil)
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	stats, err := h.service.GetPublicDiscoveryNetworkStats(r.Context(), 1)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "relay stats are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := map[string]any{
		"relays": map[string]any{
			"total": stats.Relays,
		},
		"consistency": "eventual",
	}
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) writeDiscoveryProfiles(w http.ResponseWriter, r *http.Request, surface string) {
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
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "profiles_"+surface, map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	var profilesRows []query.TrendingProfile
	switch surface {
	case "trending":
		profilesRows, err = h.service.GetTrendingProfiles(r.Context(), window, limit, offset)
	case "rising":
		profilesRows, err = h.service.GetRisingProfiles(r.Context(), window, limit, offset)
	default:
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "unsupported discovery surface")
		return
	}
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "discovery profiles are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	profiles := make([]map[string]any, 0, len(profilesRows))
	for _, profile := range profilesRows {
		profiles = append(profiles, map[string]any{
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
	payload := map[string]any{
		"surface":     surface,
		"window":      windowLabel,
		"profiles":    profiles,
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payload)
	h.cachePublicPayload(cachePolicy, payload)
	writeJSON(w, http.StatusOK, payload)
}
