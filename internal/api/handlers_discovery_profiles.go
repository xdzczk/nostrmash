package api

import (
	"net/http"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetTrendingProfiles(w http.ResponseWriter, r *http.Request) {
	h.writeDiscoveryProfiles(w, r, "trending")
}

func (h Handlers) GetRisingProfiles(w http.ResponseWriter, r *http.Request) {
	h.writeDiscoveryProfiles(w, r, "rising")
}

func (h Handlers) GetRelatedProfiles(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "profiles_related", map[string]any{
		"pubkey": pubkey,
		"limit":  limit,
	})
	if h.writePublicCachedResponse(w, cachePolicy) {
		return
	}
	related, err := h.service.GetRelatedProfiles(r.Context(), pubkey, limit)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "profile not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "related profiles are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	items := make([]map[string]any, 0, len(related))
	for _, profile := range related {
		items = append(items, map[string]any{
			"pubkey":                 profile.Pubkey,
			"topic_overlap":          profile.TopicOverlap,
			"reply_adjacency":        profile.ReplyAdjacency,
			"interaction_adjacency":  profile.InteractionAdjacency,
			"quote_repost_adjacency": profile.QuoteRepostAdjacency,
			"reasons":                profile.Reasons,
			"score":                  profile.Score,
		})
	}
	payload := map[string]any{
		"pubkey":      pubkey,
		"related":     items,
		"consistency": "eventual",
	}
	h.addDiscoveryTrustMetadata(payload)
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
