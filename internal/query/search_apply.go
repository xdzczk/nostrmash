package query

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/readmodel"
	"github.com/xdzczk/nostrmash/internal/traceutil"
)

const (
	noteRankPool            = 40
	noteRecentPool          = 25
	noteHashtagRecentPool   = 40
	profileRankPool         = 40
	noteSectionBudget       = 3500 * time.Millisecond
	profileSectionBudget    = 2000 * time.Millisecond
	documentSectionBudget   = 1500 * time.Millisecond
	postgresFallbackBudget  = 900 * time.Millisecond
	hashtagSupplementBudget = 400 * time.Millisecond
	signalLookupBudget      = 250 * time.Millisecond
	correctionLookupBudget  = 200 * time.Millisecond
	noteFreshWindow         = 24 * time.Hour
)

type hashtagFrequencyReader interface {
	HashtagDocumentFrequencies(ctx context.Context, terms []string) (map[string]int64, error)
}

type profileSignalReader interface {
	SearchProfileSignals(ctx context.Context, pubkeys []string) (map[string]readmodel.ProfileSearchSignal, error)
}

type eventCountReader interface {
	GetEventCountsByIDs(ctx context.Context, ids []string) (map[string]EventCounts, error)
}

// SearchDiagnostics is the per-request timing and scoring bag.
type SearchDiagnostics = readmodel.SearchDiagnostics

// SearchDiagnosticsSnapshot is the mutex-free copy handlers log and render.
type SearchDiagnosticsSnapshot = readmodel.SearchDiagnosticsSnapshot

// SearchSectionStat is one sub-query's timing and corpus estimate.
type SearchSectionStat = readmodel.SearchSectionStat

// WithSearchDiagnostics installs a diagnostics bag when the context does not
// already carry one. Handlers call this before any search sub-query.
func WithSearchDiagnostics(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if readmodel.SearchDiagnosticsFrom(ctx) != nil {
		return ctx
	}
	ctx, _ = readmodel.WithSearchDiagnostics(ctx)
	return ctx
}

// CurrentSearchDiagnostics copies the bag for logging and response fields.
func CurrentSearchDiagnostics(ctx context.Context) SearchDiagnosticsSnapshot {
	return readmodel.SearchDiagnosticsFrom(ctx).Snapshot()
}

// SearchPrepared runs notes, profiles, and documents with one corrected query
// and the shared ranker. A failed section becomes an empty slice; the caller
// reads surface errors from the diagnostics bag.
func (s Service) SearchPrepared(ctx context.Context, params GlobalSearchParams) (out SearchResult, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search")
	defer func() { span.End(err) }()
	if consumer, ok := s.meilisearch.(interface{ ConsumeHighlights() map[string]any }); ok {
		_ = consumer.ConsumeHighlights()
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	sort := strings.ToLower(strings.TrimSpace(params.Sort))
	if sort == "" {
		sort = "relevant"
	}

	stripped := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(params.Query), "nostr:"))
	if eventID := canonicalizeEventIdentifier(stripped); eventID.EventID != "" {
		return s.searchByEventIdentifier(ctx, eventID)
	}

	profileQuery := normalizeProfileSearchQuery(params.Query)
	text := profileQuery.NormalizedQuery
	if profileQuery.CanonicalIdentifier != "" {
		text = profileQuery.CanonicalIdentifier
	}
	if s.queryIsObviousMiss(ctx, text) {
		return s.emptySearchResult(), nil
	}
	prepareStarted := time.Now()
	text = s.prepareQuery(ctx, text)
	readmodel.SearchDiagnosticsFrom(ctx).AddDuration("notes", time.Since(prepareStarted))

	var (
		events      []json.RawMessage
		profiles    []Profile
		hashtags    []HashtagSuggestion
		relays      []string
		identities  []string
		notesErr    error
		profilesErr error
		docsErr     error
		wg          sync.WaitGroup
	)
	wg.Add(3)
	go func() {
		defer wg.Done()
		sectionCtx, cancel := context.WithTimeout(ctx, noteSectionBudget)
		defer cancel()
		events, notesErr = s.SearchNotes(sectionCtx, NotesSearchParams{
			Query:    text,
			Limit:    limit,
			Sort:     sort,
			Language: params.Language,
			Window:   params.Window,
		})
	}()
	go func() {
		defer wg.Done()
		sectionCtx, cancel := context.WithTimeout(ctx, profileSectionBudget)
		defer cancel()
		profiles, profilesErr = s.SearchProfiles(sectionCtx, ProfileSearchParams{
			Query:    text,
			Limit:    limit,
			Sort:     sort,
			Language: params.Language,
			Window:   params.Window,
		})
	}()
	go func() {
		defer wg.Done()
		sectionCtx, cancel := context.WithTimeout(ctx, documentSectionBudget)
		defer cancel()
		hashtags, relays, identities, docsErr = s.searchGlobalDocuments(sectionCtx, text, limit)
	}()
	wg.Wait()

	degraded := false
	if notesErr != nil {
		events = []json.RawMessage{}
		degraded = true
	}
	if profilesErr != nil {
		profiles = []Profile{}
		degraded = true
	}
	if docsErr != nil {
		hashtags, relays, identities = nil, nil, nil
		degraded = true
		readmodel.SearchDiagnosticsFrom(ctx).SetSectionError("hashtags", docsErr)
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	if profiles == nil {
		profiles = []Profile{}
	}
	relays = injectCanonicalRelays(relays, params.Query, text)

	if profileQuery.CanonicalIdentifier == "" && len(profiles) == 0 && len(events) > 0 && s.fallback != nil {
		profiles = s.enrichProfilesFromNoteAuthors(ctx, events, text, limit)
		profiles = s.rankProfileRows(ctx, ProfileSearchParams{
			Query:  text,
			Sort:   sort,
			Window: params.Window,
			Limit:  limit,
		}, profiles)
	}

	engine := s.SearchEngineName()
	if degraded && engine == "meilisearch" {
		engine = "degraded"
	}
	result := SearchResult{
		Events:       events,
		Profiles:     profiles,
		Hashtags:     hashtags,
		Relays:       relays,
		Identities:   identities,
		SearchEngine: engine,
	}
	if consumer, ok := s.meilisearch.(interface{ ConsumeHighlights() map[string]any }); ok {
		result.Highlights = consumer.ConsumeHighlights()
	}
	return result, nil
}

func (s Service) emptySearchResult() SearchResult {
	return SearchResult{
		Events:       []json.RawMessage{},
		Profiles:     []Profile{},
		Hashtags:     []HashtagSuggestion{},
		Relays:       []string{},
		Identities:   []string{},
		SearchEngine: s.SearchEngineName(),
	}
}

// SearchSuggestionsPrepared ranks profiles and note previews with the same
// functions as /search. Hashtags stay on the suggestion index.
func (s Service) SearchSuggestionsPrepared(ctx context.Context, params SuggestionParams) (out SearchSuggestionsResult, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search_suggestions")
	defer func() { span.End(err) }()
	started := time.Now()
	defer func() {
		readmodel.SearchDiagnosticsFrom(ctx).AddDuration("suggest", time.Since(started))
	}()

	normalized, err := normalizeSuggestionParams(params)
	if err != nil {
		return SearchSuggestionsResult{}, err
	}
	if normalized.Query == "" {
		return SearchSuggestionsResult{
			Profiles: []Profile{},
			Hashtags: []HashtagSuggestion{},
			Notes:    []json.RawMessage{},
		}, nil
	}
	profileQuery := normalizeProfileSearchQuery(normalized.Query)
	normalized.Query = profileQuery.NormalizedQuery
	if profileQuery.CanonicalIdentifier != "" {
		normalized.Query = profileQuery.CanonicalIdentifier
	}
	if !s.queryIsObviousMiss(ctx, normalized.Query) {
		normalized.Query = s.prepareQuery(ctx, normalized.Query)
	}

	var (
		profiles    []Profile
		hashtags    []HashtagSuggestion
		notes       []json.RawMessage
		profilesErr error
		hashErr     error
		wg          sync.WaitGroup
	)
	fetchNotes := !normalized.SkipNotes && !s.queryIsObviousMiss(ctx, normalized.Query)
	wg.Add(2)
	if fetchNotes {
		wg.Add(1)
	}
	go func() {
		defer wg.Done()
		if s.queryIsObviousMiss(ctx, normalized.Query) {
			profiles = []Profile{}
			return
		}
		profiles, profilesErr = s.SearchProfiles(ctx, ProfileSearchParams{
			Query:    normalized.Query,
			Limit:    normalized.Limit,
			Sort:     normalized.Sort,
			Language: normalized.Language,
			Window:   normalized.Window,
		})
	}()
	go func() {
		defer wg.Done()
		hashtags, hashErr = s.suggestHashtags(ctx, normalized.Query, normalized.Limit)
	}()
	if fetchNotes {
		go func() {
			defer wg.Done()
			noteLimit := 3
			if normalized.Limit > 0 && normalized.Limit < noteLimit {
				noteLimit = normalized.Limit
			}
			found, notesErr := s.SearchNotes(ctx, NotesSearchParams{
				Query:    normalized.Query,
				Limit:    noteLimit,
				Sort:     normalized.Sort,
				Language: normalized.Language,
				Window:   normalized.Window,
			})
			if notesErr != nil {
				readmodel.SearchDiagnosticsFrom(ctx).SetSectionError("notes", notesErr)
				return
			}
			notes = found
		}()
	}
	wg.Wait()

	if profilesErr != nil && hashErr != nil {
		return SearchSuggestionsResult{}, profilesErr
	}
	if profilesErr != nil {
		readmodel.SearchDiagnosticsFrom(ctx).SetSectionError("profiles", profilesErr)
		profiles = []Profile{}
	}
	if hashErr != nil && !IsUnsupportedCapability(hashErr) {
		readmodel.SearchDiagnosticsFrom(ctx).SetSectionError("hashtags", hashErr)
		hashtags = []HashtagSuggestion{}
	}
	if profiles == nil {
		profiles = []Profile{}
	}
	if hashtags == nil {
		hashtags = []HashtagSuggestion{}
	}
	if notes == nil {
		notes = []json.RawMessage{}
	}
	result := SearchSuggestionsResult{
		Profiles: profiles,
		Hashtags: hashtags,
		Notes:    notes,
	}
	if diag := readmodel.SearchDiagnosticsFrom(ctx); diag != nil {
		snap := diag.Snapshot()
		result.CorrectedQuery = snap.CorrectedQuery
		result.CorrectionApplied = snap.CorrectionApplied
	}
	return result, nil
}

func (s Service) suggestHashtags(ctx context.Context, query string, limit int) ([]HashtagSuggestion, error) {
	started := time.Now()
	defer func() {
		readmodel.SearchDiagnosticsFrom(ctx).AddDuration("hashtags", time.Since(started))
	}()
	if s.queryIsObviousMiss(ctx, query) {
		return []HashtagSuggestion{}, nil
	}
	var hashtags []HashtagSuggestion
	if s.meilisearch != nil {
		rows, err := s.meilisearch.SuggestHashtags(ctx, query, limit)
		if err == nil {
			hashtags = rows
		}
	}
	if len(hashtags) > 0 {
		return hashtags, nil
	}
	reader, ok := s.reader.(searchSuggestionsReader)
	if !ok {
		if s.meilisearch != nil {
			return []HashtagSuggestion{}, nil
		}
		return nil, unsupportedCapabilityError("search suggestions")
	}
	return reader.SuggestHashtags(ctx, query, limit)
}

func (s Service) prepareQuery(ctx context.Context, raw string) string {
	raw = strings.TrimSpace(raw)
	diag := readmodel.SearchDiagnosticsFrom(ctx)
	if diag != nil && diag.CorrectionAlreadyChecked() {
		return raw
	}
	corrected, ok := s.chooseQueryCorrection(ctx, raw)
	if diag != nil {
		if ok {
			diag.MarkCorrection(corrected)
		} else {
			diag.MarkCorrection("")
		}
	}
	if ok {
		return corrected
	}
	return raw
}

func (s Service) chooseQueryCorrection(ctx context.Context, raw string) (string, bool) {
	parsed := ParseSearchQuery(raw)
	if len(parsed.Phrases) > 0 || len(parsed.Terms) != 1 {
		return "", false
	}
	term := strings.TrimPrefix(foldToken(parsed.Terms[0]), "#")
	if term == "" || strings.Contains(term, ".") || is64Hex(term) || len(term) > 32 {
		return "", false
	}
	if _, isHost := CanonicalRelayHost(raw); isHost {
		return "", false
	}
	distance := 1
	if len(term) >= 8 {
		distance = 2
	}
	candidates := []string{term}
	if len(term) >= correctionMinQueryLen {
		candidates = append(candidates, Edits(term, distance)...)
	}
	freqs := s.lookupHashtagFreqs(ctx, candidates)
	s.noteHashtagFloor(ctx, freqs[term])
	if len(term) < correctionMinQueryLen {
		return "", false
	}
	lexicon := make([]TermFrequency, 0, len(freqs))
	for name, df := range freqs {
		lexicon = append(lexicon, TermFrequency{Term: name, DocumentFrequency: df})
	}
	corrected, ok := ChooseCorrection(raw, freqs[term], true, lexicon)
	if !ok {
		return "", false
	}
	s.noteHashtagFloor(ctx, freqs[corrected])
	return corrected, true
}

func (s Service) noteHashtagFloor(ctx context.Context, df int64) {
	if df <= 0 {
		return
	}
	diag := readmodel.SearchDiagnosticsFrom(ctx)
	if diag == nil {
		return
	}
	diag.SetHashtagFrequency(df)
	diag.ObserveTotal("notes", df, true)
}

func (s Service) lookupHashtagFreqs(ctx context.Context, terms []string) map[string]int64 {
	reader, ok := s.reader.(hashtagFrequencyReader)
	if !ok || len(terms) == 0 {
		return map[string]int64{}
	}
	lookupCtx, cancel := context.WithTimeout(ctx, correctionLookupBudget)
	defer cancel()
	freqs, err := reader.HashtagDocumentFrequencies(lookupCtx, terms)
	if err != nil || freqs == nil {
		return map[string]int64{}
	}
	return freqs
}

func (s Service) queryIsObviousMiss(ctx context.Context, raw string) bool {
	diag := readmodel.SearchDiagnosticsFrom(ctx)
	if diag != nil {
		if miss, ok := diag.ObviousMiss(); ok {
			return miss
		}
	}
	if !shouldShortCircuitQuery(raw) {
		if diag != nil {
			diag.SetObviousMiss(false)
		}
		return false
	}
	started := time.Now()
	miss := s.noHashtagPostings(ctx, raw)
	if diag != nil {
		diag.SetObviousMiss(miss)
		diag.AddDuration("notes", time.Since(started))
	}
	return miss
}

func shouldShortCircuitQuery(raw string) bool {
	if !ObviousEmptyToken(raw) {
		return false
	}
	term := foldToken(strings.TrimSpace(raw))
	if term == "" || strings.Contains(term, ".") || is64Hex(term) {
		return false
	}
	if _, isHost := CanonicalRelayHost(raw); isHost {
		return false
	}
	return true
}

func (s Service) noHashtagPostings(ctx context.Context, raw string) bool {
	reader, ok := s.reader.(hashtagFrequencyReader)
	if !ok {
		return false
	}
	term := strings.TrimPrefix(foldToken(strings.TrimSpace(raw)), "#")
	if term == "" {
		return false
	}
	lookupCtx, cancel := context.WithTimeout(ctx, correctionLookupBudget)
	defer cancel()
	freqs, err := reader.HashtagDocumentFrequencies(lookupCtx, []string{term})
	if err != nil {
		return false
	}
	return freqs[term] == 0
}

func (s Service) finishNotePage(ctx context.Context, params NotesSearchParams) ([]json.RawMessage, error) {
	rows, err := s.searchNotesCollected(ctx, params)
	if err != nil {
		return nil, err
	}
	ranked := s.rankNoteEvents(ctx, params, rows)
	if params.Offset == 0 {
		return capAuthorPage(ranked, params.Limit, NoteAuthorCap), nil
	}
	if params.Limit > 0 && len(ranked) > params.Limit {
		ranked = ranked[:params.Limit]
	}
	return ranked, nil
}

func (s Service) searchNotesCollected(ctx context.Context, params NotesSearchParams) ([]json.RawMessage, error) {
	fetch := params
	if s.meilisearch != nil && params.Sort == "relevant" && params.Offset == 0 && strings.TrimSpace(params.BeforeID) == "" && params.Limit > 0 && params.Limit < noteRankPool {
		fetch.Limit = noteRankPool
	}
	rows, err := s.searchNotesPage(ctx, fetch)
	if err != nil {
		return rows, err
	}
	return s.supplementFreshNotes(ctx, params, rows), nil
}

func (s Service) shouldMergeRecent(params NotesSearchParams) bool {
	return s.meilisearch != nil && params.Sort == "relevant" && params.Offset == 0 && strings.TrimSpace(params.BeforeID) == ""
}

// supplementFreshNotes blends a recent slice into a lexical page.
// A fresh Meilisearch hit only needs the engine's own latest window.
// A stale or empty page pulls the hashtag feed (single token, no language
// filter) and, if that is still stale, a short Postgres scan. Empty pages
// do not fall through to the content scan, so a term with no postings stays
// one index lookup.
func (s Service) supplementFreshNotes(ctx context.Context, params NotesSearchParams, rows []json.RawMessage) []json.RawMessage {
	if !s.shouldMergeRecent(params) {
		return rows
	}
	if len(rows) > 0 && notesAreFresh(rows, noteFreshWindow) {
		if params.Window == nil || *params.Window > 7*24*time.Hour {
			if recent := s.meiliRecentNotes(ctx, params); len(recent) > 0 {
				return mergeNoteEvents(recent, rows)
			}
		}
		return rows
	}
	merged := rows
	if token, ok := hashtagSupplementToken(params.Query); ok && strings.TrimSpace(params.Language) == "" {
		if extra := s.hashtagRecentNotes(ctx, token); len(extra) > 0 {
			merged = mergeNoteEvents(extra, merged)
			if len(rows) == 0 || notesAreFresh(merged, noteFreshWindow) {
				return merged
			}
		}
	}
	if len(merged) == 0 || notesAreFresh(merged, noteFreshWindow) {
		return merged
	}
	if stored := s.recentNotesFromStore(ctx, params); len(stored) > 0 {
		return mergeNoteEvents(stored, merged)
	}
	return merged
}

func (s Service) meiliRecentNotes(ctx context.Context, params NotesSearchParams) []json.RawMessage {
	if s.meilisearch == nil {
		return nil
	}
	window := 7 * 24 * time.Hour
	rows, err := s.meilisearch.SearchNotes(ctx, params.Query, "latest", &window, params.Language, noteRecentPool, 0)
	if err != nil {
		return nil
	}
	return rows
}

func (s Service) hashtagRecentNotes(ctx context.Context, token string) []json.RawMessage {
	lookupCtx, cancel := context.WithTimeout(ctx, hashtagSupplementBudget)
	defer cancel()
	rows, err := s.GetHashtagNotes(lookupCtx, token, "latest", "24h", noteHashtagRecentPool, 0)
	if err != nil || len(rows) == 0 {
		return nil
	}
	return trendingNotesAsEvents(rows)
}

func (s Service) recentNotesFromStore(ctx context.Context, params NotesSearchParams) []json.RawMessage {
	reader, ok := s.reader.(notesSearchReader)
	if !ok {
		return nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, postgresFallbackBudget)
	defer cancel()
	window := noteFreshWindow
	rows, err := reader.SearchNotes(lookupCtx, params.Query, "latest", &window, params.Language, noteHashtagRecentPool, 0)
	if err != nil {
		return nil
	}
	return rows
}

func hashtagSupplementToken(query string) (string, bool) {
	parsed := ParseSearchQuery(query)
	if len(parsed.Phrases) > 0 || len(parsed.Terms) != 1 {
		return "", false
	}
	token := strings.TrimPrefix(strings.TrimSpace(parsed.Terms[0]), "#")
	if token == "" || strings.ContainsAny(token, "./") {
		return "", false
	}
	return token, true
}

func filterNotesToWindow(rows []json.RawMessage, window *time.Duration) []json.RawMessage {
	if window == nil || *window <= 0 || len(rows) == 0 {
		return rows
	}
	cutoff := time.Now().Add(-*window).Unix()
	kept := make([]json.RawMessage, 0, len(rows))
	for _, raw := range rows {
		_, _, _, created, _ := eventIdentity(raw)
		if created > 0 && created < cutoff {
			continue
		}
		kept = append(kept, raw)
	}
	return kept
}

func notesAreFresh(rows []json.RawMessage, within time.Duration) bool {
	if len(rows) == 0 {
		return false
	}
	cutoff := time.Now().Add(-within).Unix()
	for _, raw := range rows {
		_, _, _, created, _ := eventIdentity(raw)
		if created >= cutoff {
			return true
		}
	}
	return false
}

func trendingNotesAsEvents(rows []TrendingNote) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.EventID) == "" {
			continue
		}
		raw, err := json.Marshal(struct {
			ID            string `json:"id"`
			Pubkey        string `json:"pubkey"`
			CreatedAt     int64  `json:"created_at"`
			Content       string `json:"content"`
			ReplyCount    int64  `json:"reply_count"`
			RepostCount   int64  `json:"repost_count"`
			ReactionCount int64  `json:"reaction_count"`
			ZapCount      int64  `json:"zap_count"`
		}{
			ID:            row.EventID,
			Pubkey:        row.AuthorPubkey,
			CreatedAt:     row.CreatedAt,
			Content:       row.Content,
			ReplyCount:    row.ReplyCount,
			RepostCount:   row.RepostCount,
			ReactionCount: row.ReactionCount,
			ZapCount:      row.ZapCount,
		})
		if err != nil {
			continue
		}
		out = append(out, raw)
	}
	return out
}

func omitRelayURLProfiles(query string, rows []Profile) []Profile {
	if _, ok := CanonicalRelayHost(query); !ok || len(rows) == 0 {
		return rows
	}
	kept := make([]Profile, 0, len(rows))
	for _, row := range rows {
		in := profileScoreInput(row)
		if NameLooksLikeURLOrHex(in.Name, in.DisplayName) {
			continue
		}
		kept = append(kept, row)
	}
	return kept
}

func mergeNoteEvents(groups ...[]json.RawMessage) []json.RawMessage {
	seen := map[string]struct{}{}
	out := make([]json.RawMessage, 0)
	for _, group := range groups {
		for _, raw := range group {
			id, _, _, _, _ := eventIdentity(raw)
			if id == "" {
				out = append(out, raw)
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, raw)
		}
	}
	return out
}

func (s Service) rankNoteEvents(ctx context.Context, params NotesSearchParams, events []json.RawMessage) []json.RawMessage {
	events = filterNotesToWindow(events, params.Window)
	if len(events) == 0 {
		return []json.RawMessage{}
	}
	parsed := parseNoteInputs(events)
	inputs := make([]NoteScoreInput, len(parsed))
	for i := range parsed {
		inputs[i] = parsed[i].input
		if diag := readmodel.SearchDiagnosticsFrom(ctx); diag != nil {
			if score := diag.NoteScore(inputs[i].ID); score > 0 {
				inputs[i].EngineScore = score
			}
		}
	}
	phrases := ParseSearchQuery(params.Query).Phrases
	if len(inputs) > 1 && !notesHaveFeatures(inputs) && len(phrases) == 0 {
		return events
	}
	if notesHaveFeatures(inputs) || len(phrases) > 0 {
		s.attachNoteSignals(ctx, inputs)
	}
	ranked := RankNotes(params.Query, inputs, RankOptions{Sort: params.Sort, Window: params.Window})
	if len(ranked) == 0 {
		return []json.RawMessage{}
	}
	return reorderNotes(parsed, ranked)
}

func reorderNotes(parsed []parsedNote, ranked []NoteScoreInput) []json.RawMessage {
	used := make([]bool, len(parsed))
	out := make([]json.RawMessage, 0, len(ranked))
	for _, note := range ranked {
		for i := range parsed {
			if used[i] {
				continue
			}
			if parsed[i].input.ID != note.ID || parsed[i].input.Pubkey != note.Pubkey {
				continue
			}
			used[i] = true
			out = append(out, parsed[i].raw)
			break
		}
	}
	return out
}

func capAuthorPage(events []json.RawMessage, limit int, cap int) []json.RawMessage {
	if len(events) == 0 {
		return []json.RawMessage{}
	}
	if limit <= 0 || limit > len(events) {
		limit = len(events)
	}
	if cap <= 0 {
		cap = NoteAuthorCap
	}
	out := make([]json.RawMessage, 0, limit)
	counts := map[string]int{}
	for _, raw := range events {
		if len(out) >= limit {
			break
		}
		id, pubkey, _, _, _ := eventIdentity(raw)
		key := pubkey
		if key == "" {
			key = "id:" + id
		}
		if counts[key] >= cap {
			continue
		}
		counts[key]++
		out = append(out, raw)
	}
	return out
}

func (s Service) attachNoteSignals(ctx context.Context, inputs []NoteScoreInput) {
	ids := make([]string, 0, len(inputs))
	pubkeys := make([]string, 0, len(inputs))
	seenID := map[string]struct{}{}
	seenKey := map[string]struct{}{}
	for _, note := range inputs {
		if note.ID != "" {
			if _, ok := seenID[note.ID]; !ok {
				seenID[note.ID] = struct{}{}
				ids = append(ids, note.ID)
			}
		}
		if note.Pubkey != "" {
			if _, ok := seenKey[note.Pubkey]; !ok {
				seenKey[note.Pubkey] = struct{}{}
				pubkeys = append(pubkeys, note.Pubkey)
			}
		}
	}
	signalCtx, cancel := context.WithTimeout(ctx, signalLookupBudget)
	defer cancel()
	if reader, ok := s.reader.(eventCountReader); ok && len(ids) > 0 {
		if counts, err := reader.GetEventCountsByIDs(signalCtx, ids); err == nil {
			for i := range inputs {
				row, ok := counts[inputs[i].ID]
				if !ok {
					continue
				}
				inputs[i].ReplyCount = row.ReplyCount
				inputs[i].RepostCount = row.RepostCount
				inputs[i].ReactionCount = row.ReactionCount
				inputs[i].ZapCount = row.ZapCount
			}
		}
	}
	authors := s.loadProfileSignals(signalCtx, pubkeys)
	for i := range inputs {
		signal, ok := authors[inputs[i].Pubkey]
		if !ok {
			continue
		}
		inputs[i].AuthorBot = inputs[i].AuthorBot || signal.Bot
		inputs[i].AuthorName = firstNonEmpty(signal.Name, inputs[i].AuthorName)
		inputs[i].AuthorDisplayName = firstNonEmpty(signal.DisplayName, inputs[i].AuthorDisplayName)
		inputs[i].AuthorNIP05 = firstNonEmpty(signal.NIP05, inputs[i].AuthorNIP05)
		rememberProfileSignal(ctx, signal)
	}
}

func (s Service) finishProfilePage(ctx context.Context, params ProfileSearchParams) ([]Profile, error) {
	rows, err := s.searchProfilesPage(ctx, params)
	if err != nil {
		return nil, err
	}
	ranked := s.rankProfileRows(ctx, params, rows)
	if params.Limit > 0 && len(rows) > params.Limit && len(ranked) > params.Limit {
		ranked = ranked[:params.Limit]
	}
	return ranked, nil
}

func (s Service) rankProfileRows(ctx context.Context, params ProfileSearchParams, profiles []Profile) []Profile {
	if len(profiles) == 0 {
		return []Profile{}
	}
	if len(profiles) == 1 {
		s.attachProfileSignals(ctx, profiles)
		return profiles
	}
	inputs := make([]ProfileScoreInput, len(profiles))
	for i := range profiles {
		inputs[i] = profileScoreInput(profiles[i])
	}
	if !profilesHaveFeatures(inputs) {
		return profiles
	}
	s.attachProfileSignals(ctx, profiles)
	for i := range profiles {
		inputs[i] = profileScoreInput(profiles[i])
		if diag := readmodel.SearchDiagnosticsFrom(ctx); diag != nil {
			if signal, ok := diag.ProfileSignal(profiles[i].Pubkey); ok {
				applyProfileSignal(&inputs[i], signal)
			}
		}
	}
	if !profilesHaveFeatures(inputs) {
		return profiles
	}
	ranked := RankProfiles(params.Query, inputs, RankOptions{Sort: params.Sort, Window: params.Window})
	return reorderProfiles(profiles, ranked)
}

func (s Service) attachProfileSignals(ctx context.Context, profiles []Profile) {
	pubkeys := make([]string, 0, len(profiles))
	seen := map[string]struct{}{}
	for _, profile := range profiles {
		if profile.Pubkey == "" {
			continue
		}
		if _, ok := seen[profile.Pubkey]; ok {
			continue
		}
		seen[profile.Pubkey] = struct{}{}
		pubkeys = append(pubkeys, profile.Pubkey)
	}
	authors := s.loadProfileSignals(ctx, pubkeys)
	for _, signal := range authors {
		rememberProfileSignal(ctx, signal)
	}
	for _, profile := range profiles {
		if _, ok := authors[profile.Pubkey]; ok {
			continue
		}
		input := profileScoreInput(profile)
		rememberProfileSignal(ctx, readmodel.ProfileSearchSignal{
			Pubkey:      input.Pubkey,
			Name:        input.Name,
			DisplayName: input.DisplayName,
			NIP05:       input.NIP05,
			Bot:         input.Bot,
		})
	}
}

func (s Service) loadProfileSignals(ctx context.Context, pubkeys []string) map[string]readmodel.ProfileSearchSignal {
	if len(pubkeys) == 0 {
		return map[string]readmodel.ProfileSearchSignal{}
	}
	reader, ok := s.reader.(profileSignalReader)
	if !ok {
		return map[string]readmodel.ProfileSearchSignal{}
	}
	signalCtx, cancel := context.WithTimeout(ctx, signalLookupBudget)
	defer cancel()
	rows, err := reader.SearchProfileSignals(signalCtx, pubkeys)
	if err != nil || rows == nil {
		return map[string]readmodel.ProfileSearchSignal{}
	}
	return rows
}

func rememberProfileSignal(ctx context.Context, signal readmodel.ProfileSearchSignal) {
	readmodel.SearchDiagnosticsFrom(ctx).PutProfileSignal(signal)
}

func applyProfileSignal(input *ProfileScoreInput, signal readmodel.ProfileSearchSignal) {
	if input == nil {
		return
	}
	if signal.Name != "" {
		input.Name = signal.Name
	}
	if signal.DisplayName != "" {
		input.DisplayName = signal.DisplayName
	}
	if signal.NIP05 != "" {
		input.NIP05 = signal.NIP05
	}
	input.FollowerCount = signal.FollowerCount
	input.RecentPostCount = signal.RecentPostCount
	if signal.RecentActivityAt != nil {
		input.RecentActivityAt = *signal.RecentActivityAt
	}
	input.Bot = input.Bot || signal.Bot
}

type parsedNote struct {
	raw   json.RawMessage
	input NoteScoreInput
}

func parseNoteInputs(events []json.RawMessage) []parsedNote {
	out := make([]parsedNote, 0, len(events))
	for _, raw := range events {
		id, pubkey, content, createdAt, counts := eventIdentity(raw)
		out = append(out, parsedNote{
			raw: raw,
			input: NoteScoreInput{
				ID:            id,
				Pubkey:        pubkey,
				CreatedAt:     createdAt,
				Content:       content,
				ReplyCount:    counts.reply,
				RepostCount:   counts.repost,
				ReactionCount: counts.reaction,
				ZapCount:      counts.zap,
			},
		})
	}
	return out
}

type parsedEventCounts struct {
	reply    int64
	repost   int64
	reaction int64
	zap      int64
}

func eventIdentity(raw json.RawMessage) (string, string, string, int64, parsedEventCounts) {
	var ev struct {
		ID            string `json:"id"`
		Pubkey        string `json:"pubkey"`
		Content       string `json:"content"`
		CreatedAt     int64  `json:"created_at"`
		ReplyCount    int64  `json:"reply_count"`
		RepostCount   int64  `json:"repost_count"`
		ReactionCount int64  `json:"reaction_count"`
		ZapCount      int64  `json:"zap_count"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return "", "", "", 0, parsedEventCounts{}
	}
	return ev.ID, ev.Pubkey, ev.Content, ev.CreatedAt, parsedEventCounts{
		reply:    ev.ReplyCount,
		repost:   ev.RepostCount,
		reaction: ev.ReactionCount,
		zap:      ev.ZapCount,
	}
}

func profileScoreInput(profile Profile) ProfileScoreInput {
	var fields struct {
		Name        string `json:"name"`
		DisplayName string `json:"display_name"`
		NIP05       string `json:"nip05"`
		About       string `json:"about"`
		Bot         any    `json:"bot"`
	}
	if len(profile.ProfileJSON) > 0 {
		_ = json.Unmarshal(profile.ProfileJSON, &fields)
	}
	return ProfileScoreInput{
		Pubkey:      profile.Pubkey,
		Name:        fields.Name,
		DisplayName: fields.DisplayName,
		NIP05:       fields.NIP05,
		About:       fields.About,
		Bot:         jsonTruthy(fields.Bot),
	}
}

func jsonTruthy(value any) bool {
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

func reorderProfiles(profiles []Profile, ranked []ProfileScoreInput) []Profile {
	used := make([]bool, len(profiles))
	out := make([]Profile, 0, len(ranked))
	for _, row := range ranked {
		for i := range profiles {
			if used[i] || profiles[i].Pubkey != row.Pubkey {
				continue
			}
			used[i] = true
			out = append(out, profiles[i])
			break
		}
	}
	return out
}

func injectCanonicalRelays(relays []string, queries ...string) []string {
	for _, query := range queries {
		host, ok := CanonicalRelayHost(query)
		if !ok {
			continue
		}
		relays = prependUnique(relays, host)
	}
	return relays
}

func prependUnique(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	filtered := make([]string, 0, len(values)+1)
	filtered = append(filtered, value)
	for _, existing := range values {
		if strings.EqualFold(strings.TrimSpace(existing), value) {
			continue
		}
		filtered = append(filtered, existing)
	}
	return filtered
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
