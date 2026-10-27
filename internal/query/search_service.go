package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/languagelabel"
	"github.com/xdzczk/nostrmash/internal/readmodel"
	"github.com/xdzczk/nostrmash/internal/traceutil"
)

func (s Service) Search(ctx context.Context, text string, limit int) (SearchResult, error) {
	return s.SearchPrepared(ctx, GlobalSearchParams{
		Query: text,
		Limit: limit,
		Sort:  "relevant",
	})
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

func (s Service) SearchSuggestions(ctx context.Context, text string, limit int) (SearchSuggestionsResult, error) {
	return s.SearchSuggestionsPrepared(ctx, SuggestionParams{
		Query: text,
		Limit: limit,
		Sort:  "relevant",
	})
}

func (s Service) SearchNotes(ctx context.Context, params NotesSearchParams) (out []json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search_notes")
	defer func() { span.End(err) }()
	started := time.Now()
	defer func() {
		readmodel.SearchDiagnosticsFrom(ctx).AddDuration("notes", time.Since(started))
	}()

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
			readmodel.SearchDiagnosticsFrom(ctx).SetSectionError("notes", getErr)
			return nil, getErr
		}
		return []json.RawMessage{raw}, nil
	}
	if s.queryIsObviousMiss(ctx, normalized.Query) {
		return []json.RawMessage{}, nil
	}
	normalized.Query = s.prepareQuery(ctx, normalized.Query)
	if normalized.Query == "" {
		return []json.RawMessage{}, nil
	}

	rows, err := s.searchNotesTrustAware(ctx, normalized)
	if err != nil {
		readmodel.SearchDiagnosticsFrom(ctx).SetSectionError("notes", err)
		return nil, err
	}
	return rows, nil
}

func (s Service) SearchProfiles(ctx context.Context, params ProfileSearchParams) (out []Profile, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search_profiles")
	defer func() { span.End(err) }()
	started := time.Now()
	defer func() {
		readmodel.SearchDiagnosticsFrom(ctx).AddDuration("profiles", time.Since(started))
	}()

	normalized, err := normalizeProfileSearchParams(params)
	if err != nil {
		return nil, err
	}
	if normalized.Query == "" {
		return []Profile{}, nil
	}
	profileQuery := normalizeProfileSearchQuery(normalized.Query)
	if profileQuery.NormalizedQuery == "" && profileQuery.CanonicalIdentifier == "" {
		return []Profile{}, nil
	}
	if profileQuery.CanonicalIdentifier != "" {
		normalized.Query = profileQuery.CanonicalIdentifier
	} else {
		if s.queryIsObviousMiss(ctx, profileQuery.NormalizedQuery) {
			return []Profile{}, nil
		}
		normalized.Query = s.prepareQuery(ctx, profileQuery.NormalizedQuery)
	}
	if normalized.Query == "" {
		return []Profile{}, nil
	}
	results, err := s.searchProfilesTrustAware(ctx, normalized)
	if err != nil {
		readmodel.SearchDiagnosticsFrom(ctx).SetSectionError("profiles", err)
		return nil, err
	}
	results = omitRelayURLProfiles(params.Query, results)
	if profileQuery.CanonicalIdentifier == "" {
		return results, nil
	}
	direct, err := s.GetProfile(ctx, profileQuery.CanonicalIdentifier)
	if err != nil {
		if IsNotFound(err) {
			return results, nil
		}
		readmodel.SearchDiagnosticsFrom(ctx).SetSectionError("profiles", err)
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
			return NotesSearchParams{}, fmt.Errorf("language must be \"und\", \"und-uncertain\", or a 2-8 letter code")
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
	started := time.Now()
	defer func() {
		elapsed := time.Since(started)
		diag := readmodel.SearchDiagnosticsFrom(ctx)
		diag.AddDuration("hashtags", elapsed)
		diag.AddDuration("relays", elapsed)
	}()
	query = strings.TrimSpace(query)
	if query == "" || s.queryIsObviousMiss(ctx, query) {
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
	return languagelabel.ValidFilter(value)
}

func normalizeSuggestionParams(params SuggestionParams) (SuggestionParams, error) {
	params.Query = strings.TrimSpace(params.Query)
	if params.Limit <= 0 {
		params.Limit = 8
	}
	if params.Limit > 20 {
		params.Limit = 20
	}
	sort := strings.ToLower(strings.TrimSpace(params.Sort))
	if sort == "" {
		sort = "relevant"
	}
	switch sort {
	case "relevant", "latest":
	default:
		return SuggestionParams{}, fmt.Errorf("sort must be one of: relevant, latest")
	}
	params.Sort = sort
	lang := strings.ToLower(strings.TrimSpace(params.Language))
	if lang != "" && !isValidLanguageToken(lang) {
		return SuggestionParams{}, fmt.Errorf("language must be \"und\", \"und-uncertain\", or a 2-8 letter code")
	}
	params.Language = lang
	return params, nil
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
	switch sort {
	case "relevant", "latest":
	default:
		return ProfileSearchParams{}, fmt.Errorf("sort must be one of: relevant, latest")
	}
	params.Sort = sort
	lang := strings.ToLower(strings.TrimSpace(params.Language))
	if lang != "" && !isValidLanguageToken(lang) {
		return ProfileSearchParams{}, fmt.Errorf("language must be \"und\", \"und-uncertain\", or a 2-8 letter code")
	}
	params.Language = lang
	return params, nil
}
