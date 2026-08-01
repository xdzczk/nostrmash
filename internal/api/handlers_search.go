package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

var (
	errInvalidNotesSearchSort    = errors.New("sort must be one of: relevant, latest")
	errInvalidProfilesSearchSort = errors.New("sort must be one of: relevant")
	errInvalidNotesSearchWindow  = errors.New("window must be one of: 24h, 7d")
	errInvalidNotesSearchLang    = errors.New("lang must be \"und\" or a 2-8 letter code")
)

// Search returns a best-effort combined event/profile search.
// Opt-in `include=suggest` folds suggestion profiles/hashtags into the same
// response so clients can avoid a separate /search/suggest round-trip.
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
	includeSuggest := searchIncludeSuggest(r.URL.Query().Get("include"))
	result, err := h.service.Search(r.Context(), queryText, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	projectedProfiles := projectProfiles(result.Profiles)
	response := map[string]any{
		"query":         queryText,
		"events":        result.Events,
		"notes":         result.Events,
		"profiles":      projectedProfiles,
		"search_engine": result.SearchEngine,
		"consistency":   "eventual",
		"section_totals": map[string]any{
			"notes":    len(result.Events),
			"profiles": len(projectedProfiles),
		},
	}
	if len(result.Highlights) > 0 {
		response["highlights"] = result.Highlights
	}
	hashtags := make([]map[string]any, 0, len(result.Hashtags))
	if len(result.Hashtags) > 0 {
		for _, hashtag := range result.Hashtags {
			hashtags = append(hashtags, map[string]any{
				"hashtag":        hashtag.Hashtag,
				"event_count":    hashtag.EventCount,
				"unique_authors": hashtag.UniqueAuthors,
			})
		}
		response["hashtags"] = hashtags
	}
	if len(result.Relays) > 0 {
		response["relays"] = result.Relays
	}
	if len(result.Identities) > 0 {
		response["identities"] = result.Identities
	}

	if includeSuggest {
		suggestLimit := limit
		if suggestLimit > 20 {
			suggestLimit = 20
		}
		if suggestLimit < 1 {
			suggestLimit = 8
		}
		suggestQuery := normalizeSuggestionQuery(queryText)
		if len(suggestQuery) >= 2 {
			if suggestions, suggestErr := h.service.SearchSuggestions(r.Context(), suggestQuery, suggestLimit); suggestErr == nil {
				suggestedProfiles := projectProfiles(suggestions.Profiles)
				response["suggested_profiles"] = suggestedProfiles
				response["profile_suggestions"] = suggestedProfiles
				suggestedHashtags := make([]map[string]any, 0, len(suggestions.Hashtags))
				for _, hashtag := range suggestions.Hashtags {
					suggestedHashtags = append(suggestedHashtags, map[string]any{
						"hashtag":        hashtag.Hashtag,
						"event_count":    hashtag.EventCount,
						"unique_authors": hashtag.UniqueAuthors,
					})
				}
				if len(suggestedHashtags) > 0 {
					response["suggested_hashtags"] = suggestedHashtags
					if len(hashtags) == 0 {
						response["hashtags"] = suggestedHashtags
						hashtags = suggestedHashtags
					}
				}
				sectionTotals := response["section_totals"].(map[string]any)
				sectionTotals["suggestions"] = len(suggestedProfiles)
				sectionTotals["hashtags"] = len(hashtags)
			}
		}
		response["include"] = []string{"suggest"}
	}

	h.addSearchTrustMetadata(response)
	writeJSON(w, http.StatusOK, response)
}

func searchIncludeSuggest(raw string) bool {
	for _, part := range strings.Split(strings.ToLower(strings.TrimSpace(raw)), ",") {
		if strings.TrimSpace(part) == "suggest" {
			return true
		}
	}
	return false
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
	language, err := parseOptionalNotesSearchLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	events, err := h.service.SearchNotes(r.Context(), query.NotesSearchParams{
		Query:    queryText,
		Limit:    limit,
		Offset:   offset,
		Sort:     sort,
		Language: language,
		Window:   window,
	})
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	response := map[string]any{
		"query":         queryText,
		"sort":          sort,
		"limit":         limit,
		"offset":        offset,
		"notes":         events,
		"search_engine": h.service.SearchEngineName(),
		"consistency":   "eventual",
	}
	if sort == "relevant" {
		h.addSearchTrustMetadata(response)
	} else {
		addOpenTrustMetadata(response)
	}
	if windowLabel != "" {
		response["window"] = windowLabel
	}
	if language != "" {
		response["lang"] = language
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
		"query":         queryText,
		"sort":          sort,
		"limit":         limit,
		"offset":        offset,
		"profiles":      projectProfiles(profiles),
		"search_engine": h.service.SearchEngineName(),
		"consistency":   "eventual",
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
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilySuggestion, "search_suggest", map[string]any{
		"q":     normalizeCacheFolded(queryText),
		"limit": limit,
	})
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		result, resultErr := h.service.SearchSuggestions(ctx, queryText, limit)
		if resultErr != nil {
			return nil, resultErr
		}
		hashtags := make([]map[string]any, 0, len(result.Hashtags))
		for _, hashtag := range result.Hashtags {
			hashtags = append(hashtags, map[string]any{
				"hashtag":        hashtag.Hashtag,
				"event_count":    hashtag.EventCount,
				"unique_authors": hashtag.UniqueAuthors,
			})
		}
		return map[string]any{
			"query":         queryText,
			"profiles":      projectProfiles(result.Profiles),
			"hashtags":      hashtags,
			"search_engine": h.service.SearchEngineName(),
			"consistency":   "eventual",
		}, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "search suggestions are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
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

func parseOptionalNotesSearchLanguage(r *http.Request) (string, error) {
	lang := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("lang")))
	if lang == "" {
		return "", nil
	}
	if lang == "und" {
		return lang, nil
	}
	if len(lang) < 2 || len(lang) > 8 {
		return "", errInvalidNotesSearchLang
	}
	for _, ch := range lang {
		if ch < 'a' || ch > 'z' {
			return "", errInvalidNotesSearchLang
		}
	}
	return lang, nil
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
