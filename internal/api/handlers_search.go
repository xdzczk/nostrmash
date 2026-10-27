package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xdzczk/nostrmash/internal/languagelabel"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/query"
)

const searchRequestBudget = 5 * time.Second

var (
	errInvalidSearchSort   = errors.New("sort must be one of: relevant, latest")
	errInvalidSearchWindow = errors.New("window must be one of: 24h, 7d, 30d, all")
	errInvalidSearchLang   = errors.New("lang must be \"und\", \"und-uncertain\", or a 2-8 letter code")
)

type searchProfileResponse struct {
	Pubkey            string          `json:"pubkey"`
	MetadataEventID   string          `json:"metadata_event_id"`
	MetadataCreatedAt int64           `json:"metadata_created_at"`
	Profile           json.RawMessage `json:"profile"`
	Name              string          `json:"name"`
	DisplayName       string          `json:"display_name,omitempty"`
	FollowerCount     int64           `json:"follower_count"`
	RecentPostCount   int64           `json:"recent_post_count"`
	RecentActivityAt  *int64          `json:"recent_activity_at"`
	NIP05             string          `json:"nip05"`
	Bot               bool            `json:"bot"`
}

type searchNotePreview struct {
	ID             string            `json:"id"`
	Pubkey         string            `json:"pubkey"`
	CreatedAt      int64             `json:"created_at"`
	ContentPreview string            `json:"content_preview"`
	MatchSpans     []searchMatchSpan `json:"match_spans"`
}

type searchMatchSpan struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// Search returns a best-effort combined event/profile search.
// Opt-in `include=suggest` folds suggestion profiles/hashtags into the same
// response so clients can avoid a separate /search/suggest round-trip.
func (h Handlers) Search(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
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
	sort, err := parseSearchSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window, windowLabel, err := parseSearchWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	language, err := parseSearchLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	includeSuggest := searchIncludeSuggest(r.URL.Query().Get("include"))
	ctx, cancel := context.WithTimeout(r.Context(), searchRequestBudget)
	defer cancel()
	ctx = query.WithSearchDiagnostics(ctx)
	defer logSearchSubqueries(ctx, "search", queryText, started)

	result, err := h.service.SearchPrepared(ctx, query.GlobalSearchParams{
		Query:    queryText,
		Limit:    limit,
		Sort:     sort,
		Language: language,
		Window:   window,
	})
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	if result.Events == nil {
		result.Events = []json.RawMessage{}
	}
	snap := query.CurrentSearchDiagnostics(ctx)
	projectedProfiles := projectSearchProfiles(snap, result.Profiles)
	hashtags := hashtagPayload(result.Hashtags)
	notesTotal, notesEstimate := noteCorpusTotal(snap, len(result.Events), limit)
	profilesTotal, profilesEstimate := sectionCorpusTotal(snap.Profiles, len(projectedProfiles), limit)
	hashtagsTotal, hashtagsEstimate := sectionCorpusTotal(snap.Hashtags, len(hashtags), limit)
	relaysTotal, relaysEstimate := sectionCorpusTotal(snap.Relays, len(result.Relays), limit)
	total := notesTotal + profilesTotal + hashtagsTotal + relaysTotal
	response := map[string]any{
		"query":              queryText,
		"events":             result.Events,
		"notes":              result.Events,
		"profiles":           projectedProfiles,
		"search_engine":      result.SearchEngine,
		"consistency":        "eventual",
		"total":              total,
		"total_is_estimate":  notesEstimate || profilesEstimate || hashtagsEstimate || relaysEstimate,
		"correction_applied": snap.CorrectionApplied,
		"section_totals": map[string]any{
			"notes":    notesTotal,
			"profiles": profilesTotal,
			"hashtags": hashtagsTotal,
			"relays":   relaysTotal,
		},
	}
	if snap.CorrectedQuery != "" {
		response["corrected_query"] = snap.CorrectedQuery
	}
	if len(result.Highlights) > 0 {
		response["highlights"] = result.Highlights
	}
	if len(hashtags) > 0 {
		response["hashtags"] = hashtags
	}
	if len(result.Relays) > 0 {
		response["relays"] = result.Relays
	}
	if len(result.Identities) > 0 {
		response["identities"] = result.Identities
	}
	if len(snap.SurfaceErrors) > 0 {
		response["surface_errors"] = snap.SurfaceErrors
		if result.SearchEngine != "degraded" {
			response["search_engine"] = "degraded"
		}
	}
	if sort != "" {
		response["sort"] = sort
	}
	if windowLabel != "" {
		response["window"] = windowLabel
	}
	if language != "" {
		response["lang"] = language
	}
	cursors := map[string]string{}
	if cursor := searchPageCursor("notes", queryText, sort, windowLabel, language, 0, len(result.Events), limit, notesTotal, result.Events); cursor != "" {
		cursors["notes"] = cursor
	}
	if cursor := searchPageCursor("profiles", queryText, sort, windowLabel, language, 0, len(projectedProfiles), limit, profilesTotal, nil); cursor != "" {
		cursors["profiles"] = cursor
	}
	if len(cursors) > 0 {
		response["surface_cursors"] = cursors
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
			if suggestions, suggestErr := h.service.SearchSuggestionsPrepared(ctx, query.SuggestionParams{
				Query:     suggestQuery,
				Limit:     suggestLimit,
				Sort:      sort,
				Language:  language,
				Window:    window,
				SkipNotes: true,
			}); suggestErr == nil {
				suggestedProfiles := projectSearchProfiles(query.CurrentSearchDiagnostics(ctx), suggestions.Profiles)
				response["suggested_profiles"] = suggestedProfiles
				response["profile_suggestions"] = suggestedProfiles
				suggestedHashtags := hashtagPayload(suggestions.Hashtags)
				if len(suggestedHashtags) > 0 {
					response["suggested_hashtags"] = suggestedHashtags
					if len(hashtags) == 0 {
						response["hashtags"] = suggestedHashtags
						hashtags = suggestedHashtags
					}
				}
				sectionTotals := response["section_totals"].(map[string]any)
				sectionTotals["suggestions"] = len(suggestedProfiles)
				if hashtagCount, _ := sectionTotals["hashtags"].(int64); hashtagCount == 0 && len(hashtags) > 0 {
					sectionTotals["hashtags"] = int64(len(hashtags))
				}
			}
		}
		response["include"] = []string{"suggest"}
	}

	if sort == "relevant" {
		h.addSearchTrustMetadata(response)
	} else {
		addOpenTrustMetadata(response)
	}
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
	started := time.Now()
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
	sort, err := parseSearchSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window, windowLabel, err := parseSearchWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	language, err := parseSearchLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	scopeHash := searchCursorScopeHash("notes", queryText, sort, windowLabel, language)
	cursor, err := decodeSearchCursor(strings.TrimSpace(r.URL.Query().Get("cursor")), scopeHash)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_cursor", searchCursorErrorMessage(err))
		return
	}
	searchParams := query.NotesSearchParams{
		Query:    queryText,
		Limit:    limit,
		Offset:   offset,
		Sort:     sort,
		Language: language,
		Window:   window,
	}
	if cursor != nil {
		if offset != 0 {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "cursor and offset are mutually exclusive")
			return
		}
		offset = cursor.Offset
		searchParams.Offset = cursor.Offset
		if sort == "latest" {
			searchParams.BeforeCreatedAt = cursor.CreatedAt
			searchParams.BeforeID = cursor.ID
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), searchRequestBudget)
	defer cancel()
	ctx = query.WithSearchDiagnostics(ctx)
	defer logSearchSubqueries(ctx, "search_notes", queryText, started)

	events, err := h.service.SearchNotes(ctx, searchParams)
	degraded := false
	if err != nil {
		recordDiscoveryDegrade(r.Context(), "search_notes", "backend", err, nil)
		events = []json.RawMessage{}
		degraded = true
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	snap := query.CurrentSearchDiagnostics(ctx)
	total, estimate := noteCorpusTotal(snap, len(events), limit)
	response := map[string]any{
		"query":              queryText,
		"sort":               sort,
		"limit":              limit,
		"offset":             offset,
		"notes":              events,
		"search_engine":      h.service.SearchEngineName(),
		"consistency":        "eventual",
		"total":              total,
		"total_is_estimate":  estimate,
		"correction_applied": snap.CorrectionApplied,
	}
	if snap.CorrectedQuery != "" {
		response["corrected_query"] = snap.CorrectedQuery
	}
	if len(snap.SurfaceErrors) > 0 {
		response["surface_errors"] = snap.SurfaceErrors
	}
	if degraded {
		response["degraded"] = true
		response["search_engine"] = "degraded"
	}
	if !degraded {
		if nextCursor := searchPageCursor("notes", queryText, sort, windowLabel, language, offset, len(events), limit, total, events); nextCursor != "" {
			response["next_cursor"] = nextCursor
		}
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
	started := time.Now()
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
	sort, err := parseSearchSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window, windowLabel, err := parseSearchWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	language, err := parseSearchLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	scopeHash := searchCursorScopeHash("profiles", queryText, sort, windowLabel, language)
	cursor, err := decodeSearchCursor(strings.TrimSpace(r.URL.Query().Get("cursor")), scopeHash)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_cursor", searchCursorErrorMessage(err))
		return
	}
	if cursor != nil {
		if offset != 0 {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "cursor and offset are mutually exclusive")
			return
		}
		offset = cursor.Offset
	}
	ctx, cancel := context.WithTimeout(r.Context(), searchRequestBudget)
	defer cancel()
	ctx = query.WithSearchDiagnostics(ctx)
	defer logSearchSubqueries(ctx, "search_profiles", queryText, started)

	profiles, err := h.service.SearchProfiles(ctx, query.ProfileSearchParams{
		Query:    queryText,
		Limit:    limit,
		Offset:   offset,
		Sort:     sort,
		Language: language,
		Window:   window,
	})
	degraded := false
	if err != nil {
		recordDiscoveryDegrade(r.Context(), "search_profiles", "backend", err, nil)
		profiles = nil
		degraded = true
	}
	snap := query.CurrentSearchDiagnostics(ctx)
	projected := projectSearchProfiles(snap, profiles)
	total, estimate := sectionCorpusTotal(snap.Profiles, len(projected), limit)
	response := map[string]any{
		"query":              queryText,
		"sort":               sort,
		"limit":              limit,
		"offset":             offset,
		"profiles":           projected,
		"search_engine":      h.service.SearchEngineName(),
		"consistency":        "eventual",
		"total":              total,
		"total_is_estimate":  estimate,
		"correction_applied": snap.CorrectionApplied,
	}
	if snap.CorrectedQuery != "" {
		response["corrected_query"] = snap.CorrectedQuery
	}
	if len(snap.SurfaceErrors) > 0 {
		response["surface_errors"] = snap.SurfaceErrors
	}
	if degraded {
		response["degraded"] = true
		response["search_engine"] = "degraded"
	}
	if !degraded {
		if nextCursor := searchPageCursor("profiles", queryText, sort, windowLabel, language, offset, len(projected), limit, total, nil); nextCursor != "" {
			response["next_cursor"] = nextCursor
		}
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

// SearchSuggest returns lightweight grouped suggestions for profiles, hashtags, and notes.
func (h Handlers) SearchSuggest(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	queryText := normalizeSuggestionQuery(r.URL.Query().Get("q"))
	limit, err := parseBoundedPositiveInt(r, "limit", 8, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	sort, err := parseSearchSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window, windowLabel, err := parseSearchWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	language, err := parseSearchLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(queryText) < 2 {
		writeJSON(w, http.StatusOK, emptySuggestResponse(queryText))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), searchRequestBudget)
	defer cancel()
	ctx = query.WithSearchDiagnostics(ctx)
	defer logSearchSubqueries(ctx, "search_suggest", queryText, started)

	cachePolicy := h.newPublicCachePolicy(publicCacheFamilySuggestion, "search_suggest", map[string]any{
		"q":      normalizeCacheFolded(queryText),
		"limit":  limit,
		"sort":   sort,
		"window": windowLabel,
		"lang":   language,
	})
	if err := h.servePublicCached(ctx, w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		result, resultErr := h.service.SearchSuggestionsPrepared(ctx, query.SuggestionParams{
			Query:    queryText,
			Limit:    limit,
			Sort:     sort,
			Language: language,
			Window:   window,
		})
		if resultErr != nil {
			if query.IsUnsupportedCapability(resultErr) {
				return nil, resultErr
			}
			recordDiscoveryDegrade(ctx, "search_suggest", "backend", resultErr, nil)
			payload := emptySuggestResponse(queryText)
			payload["search_engine"] = "degraded"
			payload["degraded"] = true
			return payload, nil
		}
		snap := query.CurrentSearchDiagnostics(ctx)
		effectiveQuery := queryText
		if snap.CorrectedQuery != "" {
			effectiveQuery = snap.CorrectedQuery
		}
		payload := map[string]any{
			"query":              queryText,
			"profiles":           projectSearchProfiles(snap, result.Profiles),
			"hashtags":           hashtagPayload(result.Hashtags),
			"notes":              notePreviews(result.Notes, effectiveQuery, 3),
			"search_engine":      h.service.SearchEngineName(),
			"consistency":        "eventual",
			"correction_applied": snap.CorrectionApplied,
		}
		if snap.CorrectedQuery != "" {
			payload["corrected_query"] = snap.CorrectedQuery
		}
		if sort != "" {
			payload["sort"] = sort
		}
		if windowLabel != "" {
			payload["window"] = windowLabel
		}
		if language != "" {
			payload["lang"] = language
		}
		return payload, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "search suggestions are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

func emptySuggestResponse(queryText string) map[string]any {
	return map[string]any{
		"query":              queryText,
		"profiles":           []searchProfileResponse{},
		"hashtags":           []map[string]any{},
		"notes":              []searchNotePreview{},
		"consistency":        "eventual",
		"correction_applied": false,
	}
}

func normalizeSuggestionQuery(raw string) string {
	queryText := strings.TrimSpace(raw)
	queryText = strings.TrimPrefix(strings.TrimPrefix(queryText, "@"), "#")
	return strings.TrimSpace(queryText)
}

func parseSearchSort(r *http.Request) (string, error) {
	sort := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort")))
	if sort == "" {
		return "relevant", nil
	}
	switch sort {
	case "relevant", "latest":
		return sort, nil
	default:
		return "", errInvalidSearchSort
	}
}

func parseSearchWindow(r *http.Request) (*time.Duration, string, error) {
	switch strings.TrimSpace(r.URL.Query().Get("window")) {
	case "":
		return nil, "", nil
	case "24h":
		window := 24 * time.Hour
		return &window, "24h", nil
	case "7d":
		window := 7 * 24 * time.Hour
		return &window, "7d", nil
	case "30d":
		window := 30 * 24 * time.Hour
		return &window, "30d", nil
	case "all":
		return nil, "all", nil
	default:
		return nil, "", errInvalidSearchWindow
	}
}

func parseSearchLanguage(r *http.Request) (string, error) {
	lang := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("lang")))
	if lang == "" {
		return "", nil
	}
	if !languagelabel.ValidFilter(lang) {
		return "", errInvalidSearchLang
	}
	return lang, nil
}

func projectSearchProfiles(snap query.SearchDiagnosticsSnapshot, rows []query.Profile) []searchProfileResponse {
	projected := make([]searchProfileResponse, 0, len(rows))
	for _, profile := range rows {
		fields := parseSearchProfileFields(profile.ProfileJSON)
		name := fields.Name
		display := fields.DisplayName
		nip05 := fields.NIP05
		bot := fields.Bot
		var followerCount int64
		var recentPostCount int64
		var recentActivity *int64
		if signal, ok := snap.ProfileSignal(profile.Pubkey); ok {
			if signal.Name != "" {
				name = signal.Name
			}
			if signal.DisplayName != "" {
				display = signal.DisplayName
			}
			if signal.NIP05 != "" {
				nip05 = signal.NIP05
			}
			followerCount = signal.FollowerCount
			recentPostCount = signal.RecentPostCount
			recentActivity = signal.RecentActivityAt
			bot = bot || signal.Bot
		}
		if name == "" {
			name = display
		}
		projected = append(projected, searchProfileResponse{
			Pubkey:            profile.Pubkey,
			MetadataEventID:   profile.MetadataEventID,
			MetadataCreatedAt: profile.MetadataCreatedAt,
			Profile:           profile.ProfileJSON,
			Name:              name,
			DisplayName:       display,
			FollowerCount:     followerCount,
			RecentPostCount:   recentPostCount,
			RecentActivityAt:  recentActivity,
			NIP05:             nip05,
			Bot:               bot,
		})
	}
	return projected
}

type searchProfileFields struct {
	Name        string
	DisplayName string
	NIP05       string
	Bot         bool
}

func parseSearchProfileFields(raw json.RawMessage) searchProfileFields {
	var payload struct {
		Name        string `json:"name"`
		DisplayName string `json:"display_name"`
		NIP05       string `json:"nip05"`
		Bot         any    `json:"bot"`
	}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &payload)
	}
	return searchProfileFields{
		Name:        payload.Name,
		DisplayName: payload.DisplayName,
		NIP05:       payload.NIP05,
		Bot:         jsonFlag(payload.Bot),
	}
}

func jsonFlag(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes":
			return true
		}
	}
	return false
}

func hashtagPayload(rows []query.HashtagSuggestion) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, hashtag := range rows {
		out = append(out, map[string]any{
			"hashtag":        hashtag.Hashtag,
			"event_count":    hashtag.EventCount,
			"unique_authors": hashtag.UniqueAuthors,
		})
	}
	return out
}

func notePreviews(events []json.RawMessage, searchQuery string, limit int) []searchNotePreview {
	if limit <= 0 {
		limit = 3
	}
	out := make([]searchNotePreview, 0, limit)
	needles := previewNeedles(searchQuery)
	for _, raw := range events {
		if len(out) >= limit {
			break
		}
		var event struct {
			ID        string `json:"id"`
			Pubkey    string `json:"pubkey"`
			CreatedAt int64  `json:"created_at"`
			Content   string `json:"content"`
		}
		if err := json.Unmarshal(raw, &event); err != nil || strings.TrimSpace(event.ID) == "" {
			continue
		}
		preview := runePreview(event.Content, 160)
		out = append(out, searchNotePreview{
			ID:             event.ID,
			Pubkey:         event.Pubkey,
			CreatedAt:      event.CreatedAt,
			ContentPreview: preview,
			MatchSpans:     matchSpans(preview, needles),
		})
	}
	return out
}

func previewNeedles(searchQuery string) []string {
	parsed := query.ParseSearchQuery(searchQuery)
	needles := make([]string, 0, len(parsed.Phrases)+len(parsed.Terms))
	needles = append(needles, parsed.Phrases...)
	needles = append(needles, parsed.Terms...)
	return needles
}

func runePreview(content string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(content)
	if len(runes) <= maxRunes {
		return content
	}
	return string(runes[:maxRunes])
}

func matchSpans(preview string, needles []string) []searchMatchSpan {
	spans := make([]searchMatchSpan, 0)
	folded := []rune(strings.ToLower(preview))
	for _, needle := range needles {
		needle = strings.TrimSpace(strings.TrimPrefix(needle, "#"))
		if needle == "" {
			continue
		}
		target := []rune(strings.ToLower(needle))
		if len(target) == 0 || len(target) > len(folded) {
			continue
		}
		for i := 0; i+len(target) <= len(folded); i++ {
			if string(folded[i:i+len(target)]) != string(target) {
				continue
			}
			spans = append(spans, searchMatchSpan{Start: i, End: i + len(target)})
			i += len(target) - 1
		}
	}
	return spans
}

func noteCorpusTotal(snap query.SearchDiagnosticsSnapshot, pageLen, limit int) (int64, bool) {
	total := snap.Notes.EstimatedTotal
	if snap.HashtagFrequency > total {
		total = snap.HashtagFrequency
	}
	if total < int64(pageLen) {
		total = int64(pageLen)
	}
	estimate := snap.Notes.TotalIsEstimate || snap.HashtagFrequency > int64(pageLen) || total > int64(pageLen)
	if !estimate && limit > 0 && pageLen >= limit {
		estimate = true
	}
	return total, estimate
}

func sectionCorpusTotal(stat query.SearchSectionStat, pageLen, limit int) (int64, bool) {
	total := stat.EstimatedTotal
	if total < int64(pageLen) {
		total = int64(pageLen)
	}
	estimate := stat.TotalIsEstimate || total > int64(pageLen)
	if !estimate && limit > 0 && pageLen >= limit {
		estimate = true
	}
	return total, estimate
}

func searchPageCursor(kind, queryText, sort, windowLabel, language string, offset, pageLen, limit int, total int64, events []json.RawMessage) string {
	if pageLen <= 0 {
		return ""
	}
	more := pageLen == limit || total > int64(offset+pageLen)
	if !more {
		return ""
	}
	nextOffset := offset + pageLen
	if nextOffset > 5000 {
		return ""
	}
	payload := searchCursorPayload{
		QHash:  searchCursorScopeHash(kind, queryText, sort, windowLabel, language),
		Sort:   sort,
		Offset: nextOffset,
	}
	if sort == "latest" && kind == "notes" {
		if createdAt, id, ok := lastEventKeyset(events); ok {
			payload.CreatedAt = createdAt
			payload.ID = id
		}
	}
	encoded, err := encodeSearchCursor(payload)
	if err != nil {
		return ""
	}
	return encoded
}

func logSearchSubqueries(ctx context.Context, route, searchQuery string, started time.Time) {
	snap := query.CurrentSearchDiagnostics(ctx)
	logging.WithRequestID(ctx, apiErrLog).Info("search_subquery_timing",
		"route", route,
		"q", truncateSearchLogQuery(searchQuery),
		"notes_ms", snap.Notes.Duration.Milliseconds(),
		"profiles_ms", snap.Profiles.Duration.Milliseconds(),
		"hashtags_ms", snap.Hashtags.Duration.Milliseconds(),
		"relays_ms", snap.Relays.Duration.Milliseconds(),
		"suggest_ms", snap.Suggest.Duration.Milliseconds(),
		"total_ms", time.Since(started).Milliseconds(),
		"notes_total", snap.Notes.EstimatedTotal,
		"profiles_total", snap.Profiles.EstimatedTotal,
		"notes_err", snap.Notes.Error,
		"profiles_err", snap.Profiles.Error,
		"hashtags_err", snap.Hashtags.Error,
	)
}

func truncateSearchLogQuery(value string) string {
	value = strings.TrimSpace(value)
	if utf8.RuneCountInString(value) <= 80 {
		return value
	}
	runes := []rune(value)
	return string(runes[:80])
}
