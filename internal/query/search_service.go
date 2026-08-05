package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/traceutil"
)

func (s Service) Search(ctx context.Context, text string, limit int) (out SearchResult, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search")
	defer func() { span.End(err) }()
	if consumer, ok := s.meilisearch.(interface{ ConsumeHighlights() map[string]any }); ok {
		// Clear highlights from any prior request before this search starts.
		_ = consumer.ConsumeHighlights()
	}

	stripped := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(text), "nostr:"))
	if eventID := canonicalizeEventIdentifier(stripped); eventID.EventID != "" {
		return s.searchByEventIdentifier(ctx, eventID)
	}

	profileQuery := normalizeProfileSearchQuery(text)
	degraded := false
	events, err := s.SearchNotes(ctx, NotesSearchParams{
		Query: profileQuery.NormalizedQuery,
		Limit: limit,
		Sort:  "relevant",
	})
	if err != nil {
		// Prefer a partial 200 over a 500 when search backends are under pressure.
		events = []json.RawMessage{}
		degraded = true
	}
	profiles, err := s.SearchProfiles(ctx, ProfileSearchParams{
		Query: profileQuery.NormalizedQuery,
		Limit: limit,
		Sort:  "relevant",
	})
	if err != nil {
		profiles = []Profile{}
		degraded = true
	}
	hashtags, relays, identities, err := s.searchGlobalDocuments(ctx, profileQuery.NormalizedQuery, limit)
	if err != nil {
		hashtags, relays, identities = nil, nil, nil
		degraded = true
	}

	if profileQuery.CanonicalIdentifier == "" && len(profiles) == 0 && len(events) > 0 && s.fallback != nil {
		profiles = s.enrichProfilesFromNoteAuthors(ctx, events, profileQuery.NormalizedQuery, limit)
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

func (s Service) enrichProfilesFromNoteAuthors(ctx context.Context, events []json.RawMessage, query string, limit int) []Profile {
	candidatePubkeys := extractCandidatePubkeysFromEvents(events, maxEnrichmentPubkeys)
	if len(candidatePubkeys) == 0 {
		return []Profile{}
	}
	infos, err := s.GetUserInfos(ctx, candidatePubkeys)
	if err != nil {
		return []Profile{}
	}
	matched := make([]Profile, 0, limit)
	for _, p := range infos.Profiles {
		if profileMatchesTextQuery(p, query) {
			matched = append(matched, p)
			if len(matched) >= limit {
				break
			}
		}
	}
	return matched
}

func (s Service) searchByEventIdentifier(ctx context.Context, eid normalizedEventIdentifier) (SearchResult, error) {
	raw, err := s.GetEventByID(ctx, eid.EventID)
	if err != nil {
		if IsNotFound(err) {
			return SearchResult{
				Events:       []json.RawMessage{},
				Profiles:     []Profile{},
				SearchEngine: s.SearchEngineName(),
			}, nil
		}
		return SearchResult{}, err
	}
	return SearchResult{
		Events:       []json.RawMessage{raw},
		Profiles:     []Profile{},
		SearchEngine: s.SearchEngineName(),
	}, nil
}

func (s Service) SearchSuggestions(ctx context.Context, text string, limit int) (out SearchSuggestionsResult, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search_suggestions")
	defer func() { span.End(err) }()

	normalized, err := normalizeSuggestionParams(text, limit)
	if err != nil {
		return SearchSuggestionsResult{}, err
	}
	if normalized.Query == "" {
		return SearchSuggestionsResult{
			Profiles: []Profile{},
			Hashtags: []HashtagSuggestion{},
		}, nil
	}
	profileQuery := normalizeProfileSearchQuery(normalized.Query)
	normalized.Query = profileQuery.NormalizedQuery
	if profileQuery.CanonicalIdentifier != "" {
		normalized.Query = profileQuery.CanonicalIdentifier
	}
	var profiles []Profile
	var hashtags []HashtagSuggestion
	meiliUsed := false
	if s.meilisearch != nil {
		meiliProfiles, profilesErr := s.meilisearch.SuggestProfiles(ctx, normalized.Query, normalized.Limit)
		meiliHashtags, hashtagsErr := s.meilisearch.SuggestHashtags(ctx, normalized.Query, normalized.Limit)
		if profilesErr == nil && hashtagsErr == nil {
			profiles = meiliProfiles
			hashtags = meiliHashtags
			meiliUsed = true
		}
	}
	suggestReader, hasSuggestReader := s.reader.(searchSuggestionsReader)
	if !meiliUsed && !hasSuggestReader {
		return SearchSuggestionsResult{}, unsupportedCapabilityError("search suggestions")
	}
	if !meiliUsed {
		var err error
		profiles, err = suggestReader.SuggestProfiles(ctx, normalized.Query, normalized.Limit)
		if err != nil {
			return SearchSuggestionsResult{}, err
		}
		hashtags, err = suggestReader.SuggestHashtags(ctx, normalized.Query, normalized.Limit)
		if err != nil {
			return SearchSuggestionsResult{}, err
		}
	} else if len(profiles) == 0 && hasSuggestReader {
		if pgProfiles, pgErr := suggestReader.SuggestProfiles(ctx, normalized.Query, normalized.Limit); pgErr == nil && len(pgProfiles) > 0 {
			profiles = pgProfiles
		}
	}
	return SearchSuggestionsResult{
		Profiles: profiles,
		Hashtags: hashtags,
	}, nil
}

func (s Service) SearchNotes(ctx context.Context, params NotesSearchParams) (out []json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search_notes")
	defer func() { span.End(err) }()

	normalized, err := normalizeNotesSearchParams(params)
	if err != nil {
		return nil, err
	}
	if normalized.Query == "" {
		return []json.RawMessage{}, nil
	}

	stripped := strings.TrimSpace(strings.TrimPrefix(normalized.Query, "nostr:"))
	if eid := canonicalizeEventIdentifier(stripped); eid.EventID != "" {
		raw, getErr := s.GetEventByID(ctx, eid.EventID)
		if getErr != nil {
			if IsNotFound(getErr) {
				return []json.RawMessage{}, nil
			}
			return nil, getErr
		}
		return []json.RawMessage{raw}, nil
	}

	return s.searchNotesTrustAware(ctx, normalized)
}

func (s Service) SearchProfiles(ctx context.Context, params ProfileSearchParams) (out []Profile, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search_profiles")
	defer func() { span.End(err) }()

	normalized, err := normalizeProfileSearchParams(params)
	if err != nil {
		return nil, err
	}
	if normalized.Query == "" {
		return []Profile{}, nil
	}
	profileQuery := normalizeProfileSearchQuery(normalized.Query)
	if profileQuery.NormalizedQuery == "" {
		return []Profile{}, nil
	}
	normalized.Query = profileQuery.NormalizedQuery
	if profileQuery.CanonicalIdentifier != "" {
		normalized.Query = profileQuery.CanonicalIdentifier
	}
	results, err := s.searchProfilesTrustAware(ctx, normalized)
	if err != nil {
		return nil, err
	}
	if profileQuery.CanonicalIdentifier == "" {
		return results, nil
	}
	direct, err := s.GetProfile(ctx, profileQuery.CanonicalIdentifier)
	if err != nil {
		if IsNotFound(err) {
			return results, nil
		}
		return nil, err
	}
	merged := make([]Profile, 0, len(results)+1)
	merged = append(merged, direct)
	for _, profile := range results {
		if profile.Pubkey == direct.Pubkey {
			continue
		}
		merged = append(merged, profile)
	}
	if len(merged) > normalized.Limit {
		merged = merged[:normalized.Limit]
	}
	return merged, nil
}

type notesSearchReader interface {
	SearchNotes(
		ctx context.Context,
		query string,
		sort string,
		window *time.Duration,
		language string,
		limit int,
		offset int,
	) ([]json.RawMessage, error)
}

type profilesSearchReader interface {
	SearchProfilesWithOptions(
		ctx context.Context,
		query string,
		sort string,
		limit int,
		offset int,
	) ([]Profile, error)
}

type searchSuggestionsReader interface {
	SuggestProfiles(ctx context.Context, query string, limit int) ([]Profile, error)
	SuggestHashtags(ctx context.Context, query string, limit int) ([]HashtagSuggestion, error)
}

type searchDocumentsReader interface {
	SearchDocuments(ctx context.Context, query string, limit int) ([]SearchDocument, error)
}

type suggestionParams struct {
	Query string
	Limit int
}

func normalizeNotesSearchParams(params NotesSearchParams) (NotesSearchParams, error) {
	params.Query = strings.TrimSpace(params.Query)
	if params.Limit <= 0 {
		params.Limit = 20
	}
	if params.Limit > 100 {
		params.Limit = 100
	}
	if params.Offset < 0 {
		return NotesSearchParams{}, fmt.Errorf("offset must be a non-negative integer")
	}
	if params.Offset > 5000 {
		return NotesSearchParams{}, fmt.Errorf("offset exceeds maximum allowed value")
	}
	sort := strings.ToLower(strings.TrimSpace(params.Sort))
	if sort == "" {
		sort = "relevant"
	}
	switch sort {
	case "relevant", "latest":
	default:
		return NotesSearchParams{}, fmt.Errorf("sort must be one of: relevant, latest")
	}
	params.Sort = sort
	lang := strings.ToLower(strings.TrimSpace(params.Language))
	if lang != "" {
		if !isValidLanguageToken(lang) {
			return NotesSearchParams{}, fmt.Errorf("language must be \"und\" or a 2-8 letter code")
		}
	}
	params.Language = lang
	return params, nil
}

func (s Service) searchGlobalDocuments(
	ctx context.Context,
	query string,
	limit int,
) ([]HashtagSuggestion, []string, []string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil, nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if s.meilisearch != nil {
		rows, err := s.meilisearch.SearchDocuments(ctx, query, limit*3)
		if err == nil {
			hashtags, relays, identities := splitSearchDocuments(rows, limit)
			return hashtags, relays, identities, nil
		}
	}
	reader, ok := s.reader.(searchDocumentsReader)
	if !ok {
		return nil, nil, nil, nil
	}
	rows, err := reader.SearchDocuments(ctx, query, limit*3)
	if err != nil {
		if IsUnsupportedCapability(err) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, err
	}
	hashtags, relays, identities := splitSearchDocuments(rows, limit)
	return hashtags, relays, identities, nil
}

func splitSearchDocuments(rows []SearchDocument, limit int) ([]HashtagSuggestion, []string, []string) {
	hashtags := make([]HashtagSuggestion, 0, limit)
	relays := make([]string, 0, limit)
	identities := make([]string, 0, limit)
	seenHashtags := make(map[string]struct{}, limit)
	seenRelays := make(map[string]struct{}, limit)
	seenIdentities := make(map[string]struct{}, limit)
	for _, row := range rows {
		switch row.EntityType {
		case "hashtag":
			tag := strings.TrimSpace(strings.TrimPrefix(row.EntityID, "#"))
			if tag == "" {
				continue
			}
			if _, ok := seenHashtags[tag]; ok {
				continue
			}
			seenHashtags[tag] = struct{}{}
			hashtags = append(hashtags, HashtagSuggestion{
				Hashtag:    tag,
				EventCount: int64(row.Popularity),
			})
		case "relay":
			relay := strings.TrimSpace(row.EntityID)
			if relay == "" {
				continue
			}
			if _, ok := seenRelays[relay]; ok {
				continue
			}
			seenRelays[relay] = struct{}{}
			relays = append(relays, relay)
		case "identity":
			identity := strings.TrimSpace(row.EntityID)
			if identity == "" {
				continue
			}
			if _, ok := seenIdentities[identity]; ok {
				continue
			}
			seenIdentities[identity] = struct{}{}
			identities = append(identities, identity)
		}
	}
	return hashtags, relays, identities
}

func isValidLanguageToken(value string) bool {
	if value == "und" {
		return true
	}
	if len(value) < 2 || len(value) > 8 {
		return false
	}
	for _, r := range value {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	return true
}

func normalizeSuggestionParams(query string, limit int) (suggestionParams, error) {
	normalized := suggestionParams{
		Query: strings.TrimSpace(query),
		Limit: limit,
	}
	if normalized.Limit <= 0 {
		normalized.Limit = 8
	}
	if normalized.Limit > 20 {
		normalized.Limit = 20
	}
	return normalized, nil
}

func normalizeProfileSearchParams(params ProfileSearchParams) (ProfileSearchParams, error) {
	params.Query = strings.TrimSpace(params.Query)
	if params.Limit <= 0 {
		params.Limit = 20
	}
	if params.Limit > 100 {
		params.Limit = 100
	}
	if params.Offset < 0 {
		return ProfileSearchParams{}, fmt.Errorf("offset must be a non-negative integer")
	}
	if params.Offset > 5000 {
		return ProfileSearchParams{}, fmt.Errorf("offset exceeds maximum allowed value")
	}
	sort := strings.ToLower(strings.TrimSpace(params.Sort))
	if sort == "" {
		sort = "relevant"
	}
	if sort != "relevant" {
		return ProfileSearchParams{}, fmt.Errorf("sort must be one of: relevant")
	}
	params.Sort = sort
	return params, nil
}
