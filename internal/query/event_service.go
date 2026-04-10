package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

type eventService struct {
	reader   EventReader
	fallback FallbackReader
	policy   fallbackPolicyRuntime
}

// NewEventService constructs an event-only orchestration service from a narrow dependency.
func NewEventService(reader EventReader) EventService {
	return eventService{reader: reader}
}

func (s Service) GetActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	return eventService{reader: s.reader, policy: s.fallbackPolicy()}.GetEventActionCounts(ctx, eventID)
}

func (s eventService) GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return ActionCounts{}, fmt.Errorf("event id is required")
	}
	counts, err := s.reader.GetEventCounts(ctx, eventID)
	if err != nil {
		return ActionCounts{}, err
	}
	return ActionCounts{
		EventID:       counts.EventID,
		ReplyCount:    counts.ReplyCount,
		ReactionCount: counts.ReactionCount,
		RepostCount:   counts.RepostCount,
		Consistency:   counts.Consistency,
	}, nil
}

func (s Service) GetEvent(ctx context.Context, id string) (json.RawMessage, error) {
	return s.GetEventByID(ctx, id)
}

func (s eventService) GetEvent(ctx context.Context, id string) (json.RawMessage, error) {
	trimmedID := strings.TrimSpace(id)
	if trimmedID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	return s.getEventWithFallback(ctx, trimmedID)
}

func (s Service) GetEvents(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return s.GetEventBatch(ctx, ids)
}

func (s eventService) GetEvents(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	normalized := normalizeUniqueStrings(ids)
	if len(normalized) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	found, err := s.reader.GetEventRawsByIDs(ctx, normalized)
	if err != nil {
		return nil, err
	}
	return s.mergeEventsWithFallback(ctx, normalized, found)
}

func (s Service) GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	return s.GetActionCounts(ctx, eventID)
}

func (s Service) GetEventByID(ctx context.Context, id string) (raw json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_by_id")
	defer func() { span.End(err) }()
	return eventService{reader: s.reader, fallback: s.fallback, policy: s.fallbackPolicy()}.GetEvent(ctx, id)
}

func (s Service) GetEventBatch(ctx context.Context, ids []string) (out map[string]json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_batch")
	defer func() { span.End(err) }()
	return eventService{reader: s.reader, fallback: s.fallback, policy: s.fallbackPolicy()}.GetEvents(ctx, ids)
}

func (s Service) Search(ctx context.Context, text string, limit int) (out SearchResult, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search")
	defer func() { span.End(err) }()
	profileQuery := normalizeProfileSearchQuery(text)
	events, err := s.SearchNotes(ctx, NotesSearchParams{
		Query: profileQuery.NormalizedQuery,
		Limit: limit,
		Sort:  "relevant",
	})
	if err != nil {
		return SearchResult{}, err
	}
	profiles, err := s.SearchProfiles(ctx, ProfileSearchParams{
		Query: profileQuery.NormalizedQuery,
		Limit: limit,
		Sort:  "relevant",
	})
	if err != nil {
		return SearchResult{}, err
	}
	hashtags, relays, identities, err := s.searchGlobalDocuments(ctx, profileQuery.NormalizedQuery, limit)
	if err != nil {
		return SearchResult{}, err
	}
	return SearchResult{
		Events:     events,
		Profiles:   profiles,
		Hashtags:   hashtags,
		Relays:     relays,
		Identities: identities,
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
	suggestReader, ok := s.reader.(searchSuggestionsReader)
	if !ok {
		return SearchSuggestionsResult{}, unsupportedCapabilityError("search suggestions")
	}

	profiles, err := suggestReader.SuggestProfiles(ctx, normalized.Query, normalized.Limit)
	if err != nil {
		return SearchSuggestionsResult{}, err
	}
	hashtags, err := suggestReader.SuggestHashtags(ctx, normalized.Query, normalized.Limit)
	if err != nil {
		return SearchSuggestionsResult{}, err
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
	reader, ok := s.reader.(searchDocumentsReader)
	if !ok {
		return nil, nil, nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := reader.SearchDocuments(ctx, query, limit*3)
	if err != nil {
		if IsUnsupportedCapability(err) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, err
	}
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
	return hashtags, relays, identities, nil
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

func (s Service) GetAuthorEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetAuthorRecentEvents(ctx, pubkey, limit)
}

func (s Service) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetAuthorReplies(ctx, pubkey, limit)
}

func (s Service) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
}

func (s Service) GetMentions(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetEventsReferencingPubkey(ctx, pubkey, limit)
}

func (s Service) GetFollowers(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetFollowersByPubkey(ctx, pubkey, limit)
}

func (s Service) GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) (EventRepliesResult, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EventRepliesResult{}, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	replies, nextCursor, err := s.reader.GetEventReplies(ctx, eventID, limit, cursor)
	if err != nil {
		return EventRepliesResult{}, err
	}
	return EventRepliesResult{
		EventID:     eventID,
		Replies:     replies,
		NextCursor:  nextCursor,
		Consistency: "eventual",
	}, nil
}

func (s Service) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) (EventAncestorsResult, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EventAncestorsResult{}, fmt.Errorf("event id is required")
	}
	if maxDepth <= 0 {
		maxDepth = 100
	}
	if maxDepth > 100 {
		maxDepth = 100
	}
	ancestors, missing, err := s.reader.GetEventAncestors(ctx, eventID, maxDepth)
	if err != nil {
		return EventAncestorsResult{}, err
	}
	return EventAncestorsResult{
		EventID:            eventID,
		Ancestors:          ancestors,
		MissingAncestorIDs: missing,
		Consistency:        "eventual",
	}, nil
}

func (s Service) GetEventWithProvenance(ctx context.Context, eventID string) (EventWithProvenanceResult, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EventWithProvenanceResult{}, fmt.Errorf("event id is required")
	}
	event, err := s.reader.GetEventWithProvenance(ctx, eventID)
	if err == nil {
		relays := make([]model.EventRelay, 0, len(event.Relays))
		for _, relay := range event.Relays {
			relays = append(relays, model.EventRelay{
				EventID:  relay.EventID,
				RelayURL: relay.RelayURL,
				SeenAt:   relay.SeenAt.UTC(),
			})
		}
		return EventWithProvenanceResult{
			Event:       event.Event,
			Relays:      relays,
			Consistency: "strong",
		}, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return EventWithProvenanceResult{}, err
	}
	raw, fallbackErr := eventService{reader: s.reader, fallback: s.fallback, policy: s.fallbackPolicy()}.GetEvent(ctx, eventID)
	if fallbackErr != nil {
		return EventWithProvenanceResult{}, store.ErrNotFound
	}
	return EventWithProvenanceResult{
		Event:       raw,
		Relays:      []model.EventRelay{},
		Consistency: "eventual",
	}, nil
}

func (s Service) GetEventSeenOn(ctx context.Context, eventID string) (EventSeenOnResult, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EventSeenOnResult{}, fmt.Errorf("event id is required")
	}
	seenOn, err := s.reader.GetEventSeenOn(ctx, eventID)
	if err != nil {
		return EventSeenOnResult{}, err
	}
	out := make([]model.EventRelay, 0, len(seenOn))
	for _, relay := range seenOn {
		out = append(out, model.EventRelay{
			EventID:  relay.EventID,
			RelayURL: relay.RelayURL,
			SeenAt:   relay.SeenAt.UTC(),
		})
	}
	return EventSeenOnResult{
		EventID: eventID,
		SeenOn:  out,
	}, nil
}

func (s Service) GetRelaysHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	rows, err := s.reader.ListRelayHealth(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]model.IngestCheckpoint, 0, len(rows))
	for _, row := range rows {
		checkpoint := row
		checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
		if checkpoint.EOSESeenAt != nil {
			eoseSeenAt := checkpoint.EOSESeenAt.UTC()
			checkpoint.EOSESeenAt = &eoseSeenAt
		}
		out = append(out, checkpoint)
	}
	return out, nil
}

func (s Service) GetZaps(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.userZaps; r != nil {
		return r.GetUserZaps(ctx, pubkey, limit, false)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9735, pubkey, limit)
}

func (s Service) GetHighlights(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9802, pubkey, limit)
}

func (s Service) GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.highlightsByEventID; r != nil {
		return r.GetHighlightsByEventID(ctx, eventID, limit)
	}
	return nil, unsupportedCapabilityError("highlights by event id")
}

func (s Service) GetHighlightsByATarget(
	ctx context.Context,
	kind int,
	pubkey string,
	identifier string,
	limit int,
) ([]json.RawMessage, error) {
	if r := s.capabilities.event.highlightsByATarget; r != nil {
		return r.GetHighlightsByATarget(ctx, kind, pubkey, identifier, limit)
	}
	return nil, unsupportedCapabilityError("highlights by a-target")
}

func (s Service) GetUserZapsBySats(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.userZaps; r != nil {
		return r.GetUserZaps(ctx, pubkey, limit, true)
	}
	return s.GetZaps(ctx, pubkey, limit)
}

func (s Service) GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.eventZapsBySats; r != nil {
		return r.GetEventZapsBySats(ctx, eventID, limit)
	}
	return nil, unsupportedCapabilityError("event zaps by sats")
}
