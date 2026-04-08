package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

var errInvalidTrendingWindow = errors.New("window must be one of: 24h, 7d")

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
	cacheKey := fmt.Sprintf("discovery:notes:trending:window=%s:limit=%d:offset=%d", windowLabel, limit, offset)
	if h.writeDiscoveryCachedResponse(w, cacheKey) {
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
	h.cacheDiscoveryPayload(cacheKey, payloadResponse, h.cacheConfig.TrendingTTL)
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
	cacheKey := fmt.Sprintf("discovery:hashtags:trending:window=%s:limit=%d:offset=%d", windowLabel, limit, offset)
	if h.writeDiscoveryCachedResponse(w, cacheKey) {
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
	h.cacheDiscoveryPayload(cacheKey, payloadResponse, h.cacheConfig.TrendingTTL)
	writeJSON(w, http.StatusOK, payloadResponse)
}

func parseTrendingHashtagWindow(r *http.Request) (time.Duration, string, error) {
	return parseTrendingWindow(r)
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
	cacheKey := fmt.Sprintf("discovery:stats:network:hashtag_limit=%d", hashtagLimit)
	if h.writeDiscoveryCachedResponse(w, cacheKey) {
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
	payload := map[string]any{
		"network":     network,
		"consistency": "eventual",
	}
	h.cacheDiscoveryPayload(cacheKey, payload, h.cacheConfig.PublicStatsTTL)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetContentStats(w http.ResponseWriter, r *http.Request) {
	hashtagLimit, err := parseBoundedPositiveInt(r, "hashtag_limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cacheKey := fmt.Sprintf("discovery:stats:content:hashtag_limit=%d", hashtagLimit)
	if h.writeDiscoveryCachedResponse(w, cacheKey) {
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
	payload := map[string]any{
		"content":     content,
		"consistency": "eventual",
	}
	h.cacheDiscoveryPayload(cacheKey, payload, h.cacheConfig.PublicStatsTTL)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetRelayStats(w http.ResponseWriter, r *http.Request) {
	cacheKey := "discovery:stats:relays"
	if h.writeDiscoveryCachedResponse(w, cacheKey) {
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
	h.cacheDiscoveryPayload(cacheKey, payload, h.cacheConfig.PublicStatsTTL)
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
	cacheKey := fmt.Sprintf("discovery:profiles:%s:window=%s:limit=%d:offset=%d", surface, windowLabel, limit, offset)
	if h.writeDiscoveryCachedResponse(w, cacheKey) {
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
	h.cacheDiscoveryPayload(cacheKey, payload, h.cacheConfig.TrendingTTL)
	writeJSON(w, http.StatusOK, payload)
}
