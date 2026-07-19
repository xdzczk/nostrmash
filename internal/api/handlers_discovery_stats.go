package api

import (
	"net/http"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetNetworkStats(w http.ResponseWriter, r *http.Request) {
	hashtagLimit, err := parseBoundedPositiveInt(r, "hashtag_limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyStats, "network_stats", map[string]any{
		"hashtag_limit": hashtagLimit,
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		stats, statsErr := h.service.GetPublicDiscoveryNetworkStats(r.Context(), hashtagLimit)
		if statsErr != nil {
			return nil, statsErr
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
			"relays": buildRelayStatsPayload(stats),
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
		return map[string]any{
			"network":     network,
			"consistency": "eventual",
		}, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "network stats are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
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
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		stats, statsErr := h.service.GetPublicDiscoveryNetworkStats(r.Context(), hashtagLimit)
		if statsErr != nil {
			return nil, statsErr
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
		return map[string]any{
			"content":     content,
			"consistency": "eventual",
		}, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "content stats are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

func (h Handlers) GetRelayStats(w http.ResponseWriter, r *http.Request) {
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyStats, "relay_stats", nil)
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		stats, statsErr := h.service.GetPublicDiscoveryNetworkStats(r.Context(), 1)
		if statsErr != nil {
			return nil, statsErr
		}
		return map[string]any{
			"relays":      buildRelayStatsPayload(stats),
			"consistency": "eventual",
		}, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "relay stats are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}
