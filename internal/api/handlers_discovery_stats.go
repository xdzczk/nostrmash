package api

import (
	"context"
	"net/http"
	"strings"

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
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		stats, statsErr := h.service.GetPublicDiscoveryNetworkStats(ctx, hashtagLimit)
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
			"computed_at": stats.ComputedAt,
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
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		stats, statsErr := h.service.GetPublicDiscoveryNetworkStats(ctx, hashtagLimit)
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
			"computed_at": stats.ComputedAt,
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
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		stats, statsErr := h.service.GetPublicDiscoveryNetworkStats(ctx, 1)
		if statsErr != nil {
			return nil, statsErr
		}
		return map[string]any{
			"relays":      buildRelayStatsPayload(stats),
			"computed_at": stats.ComputedAt,
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

func (h Handlers) GetDiscoveryStatsSeries(w http.ResponseWriter, r *http.Request) {
	metric := strings.TrimSpace(r.URL.Query().Get("metric"))
	switch metric {
	case "note_volume", "active_authors", "relay_events":
	case "":
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "metric is required")
		return
	default:
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "metric must be one of note_volume, active_authors, relay_events")
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window == "" {
		window = "7d"
	}
	if window != "7d" && window != "30d" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "window must be one of 7d, 30d")
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyStats, "discovery_stats_series", map[string]any{
		"metric": metric,
		"window": window,
	})
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		series, seriesErr := h.service.GetDiscoveryStatsSeries(ctx, metric, window)
		if seriesErr != nil {
			return nil, seriesErr
		}
		points := make([]map[string]any, 0, len(series.Points))
		for _, point := range series.Points {
			points = append(points, map[string]any{
				"t": point.T.Unix(),
				"v": point.V,
			})
		}
		payload := map[string]any{
			"metric":      series.Metric,
			"window":      series.Window,
			"computed_at": series.ComputedAt,
			"points":      points,
			"consistency": "eventual",
		}
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "discovery stats series are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}
