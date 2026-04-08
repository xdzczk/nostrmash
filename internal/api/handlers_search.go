package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

var (
	errInvalidNotesSearchSort    = errors.New("sort must be one of: relevant, latest")
	errInvalidProfilesSearchSort = errors.New("sort must be one of: relevant")
	errInvalidNotesSearchWindow  = errors.New("window must be one of: 24h, 7d")
)

// Search returns a best-effort combined event/profile search.
func (h Handlers) Search(w http.ResponseWriter, r *http.Request) {
	queryText := strings.TrimSpace(r.URL.Query().Get("q"))
	if queryText == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "q is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	result, err := h.service.Search(r.Context(), queryText, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	projectedProfiles := projectProfiles(result.Profiles)
	response := map[string]any{
		"query":       queryText,
		"events":      result.Events,
		"profiles":    projectedProfiles,
		"consistency": "eventual",
	}
	h.addSearchTrustMetadata(response)
	writeJSON(w, http.StatusOK, response)
}

// SearchNotes returns note-only search results with pagination and minimal filtering options.
func (h Handlers) SearchNotes(w http.ResponseWriter, r *http.Request) {
	queryText := strings.TrimSpace(r.URL.Query().Get("q"))
	if queryText == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "q is required")
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
	sort, err := parseNotesSearchSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window, windowLabel, err := parseOptionalNotesSearchWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	events, err := h.service.SearchNotes(r.Context(), query.NotesSearchParams{
		Query:  queryText,
		Limit:  limit,
		Offset: offset,
		Sort:   sort,
		Window: window,
	})
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	response := map[string]any{
		"query":       queryText,
		"sort":        sort,
		"limit":       limit,
		"offset":      offset,
		"notes":       events,
		"consistency": "eventual",
	}
	if sort == "relevant" {
		h.addSearchTrustMetadata(response)
	} else {
		addOpenTrustMetadata(response)
	}
	if windowLabel != "" {
		response["window"] = windowLabel
	}
	writeJSON(w, http.StatusOK, response)
}

// SearchProfiles returns profile-only search results with pagination and minimal sorting options.
func (h Handlers) SearchProfiles(w http.ResponseWriter, r *http.Request) {
	queryText := strings.TrimSpace(r.URL.Query().Get("q"))
	if queryText == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "q is required")
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
	sort, err := parseProfilesSearchSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	profiles, err := h.service.SearchProfiles(r.Context(), query.ProfileSearchParams{
		Query:  queryText,
		Limit:  limit,
		Offset: offset,
		Sort:   sort,
	})
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	response := map[string]any{
		"query":       queryText,
		"sort":        sort,
		"limit":       limit,
		"offset":      offset,
		"profiles":    projectProfiles(profiles),
		"consistency": "eventual",
	}
	h.addSearchTrustMetadata(response)
	writeJSON(w, http.StatusOK, response)
}

// SearchSuggest returns lightweight grouped suggestions for profiles and hashtags.
func (h Handlers) SearchSuggest(w http.ResponseWriter, r *http.Request) {
	queryText := normalizeSuggestionQuery(r.URL.Query().Get("q"))
	limit, err := parseBoundedPositiveInt(r, "limit", 8, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(queryText) < 2 {
		writeJSON(w, http.StatusOK, map[string]any{
			"query":       queryText,
			"profiles":    []profileResponse{},
			"hashtags":    []map[string]any{},
			"consistency": "eventual",
		})
		return
	}
	cacheKey := fmt.Sprintf("search:suggest:q=%s:limit=%d", queryText, limit)
	if h.writeDiscoveryCachedResponse(w, cacheKey) {
		return
	}
	result, err := h.service.SearchSuggestions(r.Context(), queryText, limit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "search suggestions are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	hashtags := make([]map[string]any, 0, len(result.Hashtags))
	for _, hashtag := range result.Hashtags {
		hashtags = append(hashtags, map[string]any{
			"hashtag":        hashtag.Hashtag,
			"event_count":    hashtag.EventCount,
			"unique_authors": hashtag.UniqueAuthors,
		})
	}
	payload := map[string]any{
		"query":       queryText,
		"profiles":    projectProfiles(result.Profiles),
		"hashtags":    hashtags,
		"consistency": "eventual",
	}
	h.cacheDiscoveryPayload(cacheKey, payload, h.cacheConfig.TrendingTTL)
	writeJSON(w, http.StatusOK, payload)
}

func normalizeSuggestionQuery(raw string) string {
	queryText := strings.TrimSpace(raw)
	queryText = strings.TrimPrefix(strings.TrimPrefix(queryText, "@"), "#")
	return strings.TrimSpace(queryText)
}

func parseNotesSearchSort(r *http.Request) (string, error) {
	sort := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort")))
	if sort == "" {
		return "relevant", nil
	}
	switch sort {
	case "relevant", "latest":
		return sort, nil
	default:
		return "", errInvalidNotesSearchSort
	}
}

func parseProfilesSearchSort(r *http.Request) (string, error) {
	sort := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort")))
	if sort == "" {
		return "relevant", nil
	}
	if sort != "relevant" {
		return "", errInvalidProfilesSearchSort
	}
	return sort, nil
}

func parseOptionalNotesSearchWindow(r *http.Request) (*time.Duration, string, error) {
	switch strings.TrimSpace(r.URL.Query().Get("window")) {
	case "":
		return nil, "", nil
	case "24h":
		window := 24 * time.Hour
		return &window, "24h", nil
	case "7d":
		window := 7 * 24 * time.Hour
		return &window, "7d", nil
	default:
		return nil, "", errInvalidNotesSearchWindow
	}
}

func projectProfiles(rows []query.Profile) []profileResponse {
	projectedProfiles := make([]profileResponse, 0, len(rows))
	for _, profile := range rows {
		projectedProfiles = append(projectedProfiles, profileResponse{
			Pubkey:            profile.Pubkey,
			MetadataEventID:   profile.MetadataEventID,
			MetadataCreatedAt: profile.MetadataCreatedAt,
			Profile:           profile.ProfileJSON,
		})
	}
	return projectedProfiles
}
