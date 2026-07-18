package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetAuthorAnalyticsSummary(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	summary, err := h.service.GetAuthorAnalyticsSummary(r.Context(), pubkey)
	if err != nil {
		if strings.Contains(err.Error(), "pubkey is required") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (h Handlers) GetAuthorAnalyticsTopics(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	items, windowDays, err := h.service.GetAuthorTopicStats(r.Context(), pubkey, window, limit)
	if err != nil {
		if strings.Contains(err.Error(), "window must be one of") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey": pubkey,
		"window": formatWindowDays(windowDays),
		"items":  items,
	})
}

func (h Handlers) GetAuthorAnalyticsGroupedNotes(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	groupBy := strings.TrimSpace(r.URL.Query().Get("group_by"))
	groupKey := strings.TrimSpace(r.URL.Query().Get("group_key"))
	metadataTag := strings.TrimSpace(r.URL.Query().Get("metadata_tag"))
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	topNotesLimit, err := parseBoundedPositiveInt(r, "top_notes_limit", 5, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	topicsLimit, err := parseBoundedPositiveInt(r, "topics_limit", 5, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	summary, err := h.service.GetGroupedNoteAnalytics(
		r.Context(),
		pubkey,
		window,
		groupBy,
		groupKey,
		metadataTag,
		topNotesLimit,
		topicsLimit,
	)
	if err != nil {
		if strings.Contains(err.Error(), "window must be one of") ||
			strings.Contains(err.Error(), "group_by must be one of") ||
			strings.Contains(err.Error(), "group_key is required") ||
			strings.Contains(err.Error(), "group_key must be") ||
			strings.Contains(err.Error(), "metadata_tag is required") ||
			strings.Contains(err.Error(), "metadata_tag must be one of") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "grouped note analytics are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (h Handlers) GetAuthorAnalyticsMediaMix(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	stats, windowDays, err := h.service.GetAuthorMediaMix(r.Context(), pubkey, window)
	if err != nil {
		if strings.Contains(err.Error(), "window must be one of") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":    pubkey,
		"window":    formatWindowDays(windowDays),
		"media_mix": stats,
	})
}

func (h Handlers) GetAuthorAnalyticsActivityWindows(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	result, windowDays, err := h.service.GetAuthorActivityWindows(r.Context(), pubkey, window)
	if err != nil {
		if strings.Contains(err.Error(), "window must be one of") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":   pubkey,
		"window":   formatWindowDays(windowDays),
		"timezone": result.Timezone,
		"by_hour":  result.ByHour,
		"by_day":   result.ByDay,
		"heatmap":  result.Heatmap,
	})
}

func (h Handlers) GetAuthorAnalyticsPostingPatterns(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	result, windowDays, err := h.service.GetAuthorPostingPatterns(r.Context(), pubkey, window)
	if err != nil {
		if strings.Contains(err.Error(), "window must be one of") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":   pubkey,
		"window":   formatWindowDays(windowDays),
		"timezone": result.Timezone,
		"by_hour":  result.ByHour,
		"by_day":   result.ByDay,
		"heatmap":  result.Heatmap,
	})
}

func (h Handlers) GetAuthorAnalyticsTopNotes(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	items, windowDays, err := h.service.GetAuthorTopNotes(r.Context(), pubkey, window, limit)
	if err != nil {
		if strings.Contains(err.Error(), "window must be one of") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "author top notes are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":         pubkey,
		"window":         formatWindowDays(windowDays),
		"weight_formula": authorAnalyticsWeightFormula(),
		"items":          items,
	})
}

func (h Handlers) GetAuthorAnalyticsPerformanceSummary(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	summary, windowDays, err := h.service.GetAuthorPerformanceSummary(r.Context(), pubkey, window)
	if err != nil {
		if strings.Contains(err.Error(), "window must be one of") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "author performance summary is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":         pubkey,
		"window":         formatWindowDays(windowDays),
		"weight_formula": authorAnalyticsWeightFormula(),
		"summary":        summary,
	})
}

func (h Handlers) GetAuthorAnalyticsRecycleCandidates(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	includeReplies, err := parseBoolQuery(r, "include_replies", false)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	minPerformancePercentile, err := parseBoundedFloat(r, "min_performance_percentile", 70, 0, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	minAge := strings.TrimSpace(r.URL.Query().Get("min_age"))
	items, filters, err := h.service.GetAuthorRecycleCandidates(
		r.Context(),
		pubkey,
		window,
		minAge,
		limit,
		minPerformancePercentile,
		includeReplies,
	)
	if err != nil {
		if strings.Contains(err.Error(), "window must be one of") ||
			strings.Contains(err.Error(), "min_age must be one of") ||
			strings.Contains(err.Error(), "min_age must be less than window") ||
			strings.Contains(err.Error(), "min_performance_percentile must be between 0 and 100") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "author recycle candidates are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":  pubkey,
		"filters": filters,
		"items":   items,
	})
}

func authorAnalyticsWeightFormula() map[string]any {
	return map[string]any{
		"weighted_engagement": "reply_count*3 + repost_count*2 + reaction_count + zap_count*2 + (zap_msats/100000)",
		"reply_weight":        3,
		"repost_weight":       2,
		"reaction_weight":     1,
		"zap_weight":          2,
		"zap_msats_divisor":   100000,
	}
}

func formatWindowDays(windowDays int) string {
	return fmt.Sprintf("%dd", windowDays)
}

func parseBoundedFloat(r *http.Request, key string, defaultValue float64, minValue float64, maxValue float64) (float64, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number", key)
	}
	if parsed < minValue || parsed > maxValue {
		return 0, fmt.Errorf("%s must be between %g and %g", key, minValue, maxValue)
	}
	return parsed, nil
}
