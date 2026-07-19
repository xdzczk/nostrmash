package query

import (
	"context"
	"encoding/json"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type legacyReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventWithProvenance(ctx context.Context, id string) (readmodel.EventWithProvenance, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error)
	GetProfileByPubkey(ctx context.Context, pubkey string) (readmodel.ProfileProjection, error)
	GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]readmodel.ProfileProjection, error)
	GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (readmodel.ProfilePublicStatsProjection, error)
	GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventCounts(ctx context.Context, eventID string) (readmodel.EventCounts, error)
	GetEventReplies(ctx context.Context, eventID string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
	GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
	ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error)
	GetContactListByPubkey(ctx context.Context, pubkey string) (readmodel.ContactListProjection, error)
	GetRelayListByPubkey(ctx context.Context, pubkey string) (readmodel.RelayListProjection, error)
	SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error)
	SearchProfiles(ctx context.Context, query string, limit int) ([]readmodel.ProfileProjection, error)
	GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
	GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
}

type legacyNotesSearchReader interface {
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

type legacyProfilesSearchReader interface {
	SearchProfilesWithOptions(
		ctx context.Context,
		query string,
		sort string,
		limit int,
		offset int,
	) ([]readmodel.ProfileProjection, error)
}

type legacySearchSuggestionsReader interface {
	SuggestProfiles(ctx context.Context, query string, limit int) ([]readmodel.ProfileProjection, error)
	SuggestHashtags(ctx context.Context, query string, limit int) ([]readmodel.TrendingHashtag, error)
}

type legacySearchDocumentsReader interface {
	SearchDocuments(ctx context.Context, query string, limit int) ([]readmodel.SearchDocumentProjection, error)
}

type legacyReaderAdapter struct {
	legacy legacyReader
}

type legacyDescendingThreadWindowReader interface {
	GetEventRepliesDescending(ctx context.Context, eventID string, limit int, cursor *readmodel.EventOrderCursor, offset int) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
}

func (a legacyReaderAdapter) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	return a.legacy.GetEventRawByID(ctx, id)
}

func (a legacyReaderAdapter) GetEventWithProvenance(ctx context.Context, id string) (EventWithProvenance, error) {
	row, err := a.legacy.GetEventWithProvenance(ctx, id)
	if err != nil {
		return EventWithProvenance{}, err
	}
	return eventWithProvenanceFromStore(row), nil
}

func (a legacyReaderAdapter) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return a.legacy.GetEventRawsByIDs(ctx, ids)
}

func (a legacyReaderAdapter) GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error) {
	return a.legacy.GetEventSeenOn(ctx, id)
}

func (a legacyReaderAdapter) GetProfileByPubkey(ctx context.Context, pubkey string) (Profile, error) {
	row, err := a.legacy.GetProfileByPubkey(ctx, pubkey)
	if err != nil {
		return Profile{}, err
	}
	return profileFromStore(row), nil
}

func (a legacyReaderAdapter) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	rows, err := a.legacy.GetProfilesByPubkeys(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Profile, len(rows))
	for pubkey, row := range rows {
		out[pubkey] = profileFromStore(row)
	}
	return out, nil
}

func (a legacyReaderAdapter) GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (ProfilePublicStats, error) {
	row, err := a.legacy.GetProfilePublicStatsByPubkey(ctx, pubkey)
	if err != nil {
		return ProfilePublicStats{}, err
	}
	return profilePublicStatsFromStore(row), nil
}

func (a legacyReaderAdapter) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetAuthorRecentEvents(ctx, pubkey, limit)
}

func (a legacyReaderAdapter) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetAuthorReplies(ctx, pubkey, limit)
}

func (a legacyReaderAdapter) GetEventCounts(ctx context.Context, eventID string) (EventCounts, error) {
	row, err := a.legacy.GetEventCounts(ctx, eventID)
	if err != nil {
		return EventCounts{}, err
	}
	return eventCountsFromStore(row), nil
}

func (a legacyReaderAdapter) GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error) {
	replies, next, err := a.legacy.GetEventReplies(ctx, eventID, limit, eventCursorToStore(cursor))
	if err != nil {
		return nil, nil, err
	}
	return replies, eventCursorFromStore(next), nil
}

func (a legacyReaderAdapter) GetEventRepliesDescending(ctx context.Context, eventID string, limit int, cursor *EventCursor, offset int) ([]json.RawMessage, *EventCursor, error) {
	descReader, ok := a.legacy.(legacyDescendingThreadWindowReader)
	if !ok {
		return nil, nil, unsupportedCapabilityError("thread descending replies")
	}
	replies, next, err := descReader.GetEventRepliesDescending(ctx, eventID, limit, eventCursorToStore(cursor), offset)
	if err != nil {
		return nil, nil, err
	}
	return replies, eventCursorFromStore(next), nil
}

func (a legacyReaderAdapter) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
	return a.legacy.GetEventAncestors(ctx, eventID, maxDepth)
}

func (a legacyReaderAdapter) ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	return a.legacy.ListRelayHealth(ctx)
}

func (a legacyReaderAdapter) GetContactListByPubkey(ctx context.Context, pubkey string) (ContactList, error) {
	row, err := a.legacy.GetContactListByPubkey(ctx, pubkey)
	if err != nil {
		return ContactList{}, err
	}
	return contactListFromStore(row), nil
}

func (a legacyReaderAdapter) GetRelayListByPubkey(ctx context.Context, pubkey string) (RelayList, error) {
	row, err := a.legacy.GetRelayListByPubkey(ctx, pubkey)
	if err != nil {
		return RelayList{}, err
	}
	return relayListFromStore(row), nil
}

func (a legacyReaderAdapter) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	return a.legacy.SearchEventsByContent(ctx, query, limit)
}

func (a legacyReaderAdapter) SearchProfiles(ctx context.Context, query string, limit int) ([]Profile, error) {
	rows, err := a.legacy.SearchProfiles(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Profile, 0, len(rows))
	for _, row := range rows {
		out = append(out, profileFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) SearchNotes(
	ctx context.Context,
	query string,
	sort string,
	window *time.Duration,
	language string,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if advanced, ok := a.legacy.(legacyNotesSearchReader); ok {
		return advanced.SearchNotes(ctx, query, sort, window, language, limit, offset)
	}
	if sort == "relevant" && window == nil && offset == 0 && language == "" {
		return a.legacy.SearchEventsByContent(ctx, query, limit)
	}
	return nil, unsupportedCapabilityError("advanced notes search")
}

func (a legacyReaderAdapter) SearchProfilesWithOptions(
	ctx context.Context,
	query string,
	sort string,
	limit int,
	offset int,
) ([]Profile, error) {
	if advanced, ok := a.legacy.(legacyProfilesSearchReader); ok {
		rows, err := advanced.SearchProfilesWithOptions(ctx, query, sort, limit, offset)
		if err != nil {
			return nil, err
		}
		out := make([]Profile, 0, len(rows))
		for _, row := range rows {
			out = append(out, profileFromStore(row))
		}
		return out, nil
	}
	if sort == "relevant" && offset == 0 {
		return a.SearchProfiles(ctx, query, limit)
	}
	return nil, unsupportedCapabilityError("advanced profile search")
}

func (a legacyReaderAdapter) SuggestProfiles(ctx context.Context, query string, limit int) ([]Profile, error) {
	reader, ok := a.legacy.(legacySearchSuggestionsReader)
	if !ok {
		return nil, unsupportedCapabilityError("search profile suggestions")
	}
	rows, err := reader.SuggestProfiles(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Profile, 0, len(rows))
	for _, row := range rows {
		out = append(out, profileFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) SuggestHashtags(ctx context.Context, query string, limit int) ([]HashtagSuggestion, error) {
	reader, ok := a.legacy.(legacySearchSuggestionsReader)
	if !ok {
		return nil, unsupportedCapabilityError("search hashtag suggestions")
	}
	rows, err := reader.SuggestHashtags(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]HashtagSuggestion, 0, len(rows))
	for _, row := range rows {
		out = append(out, HashtagSuggestion{
			Hashtag:       row.Hashtag,
			EventCount:    row.EventCount,
			UniqueAuthors: row.UniqueAuthors,
		})
	}
	return out, nil
}

func (a legacyReaderAdapter) SearchDocuments(ctx context.Context, query string, limit int) ([]SearchDocument, error) {
	reader, ok := a.legacy.(legacySearchDocumentsReader)
	if !ok {
		return nil, unsupportedCapabilityError("search documents")
	}
	rows, err := reader.SearchDocuments(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]SearchDocument, 0, len(rows))
	for _, row := range rows {
		out = append(out, searchDocumentFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
}

func (a legacyReaderAdapter) GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetEventsReferencingPubkey(ctx, targetPubkey, limit)
}

func (a legacyReaderAdapter) GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetFollowersByPubkey(ctx, targetPubkey, limit)
}

type legacyFallbackReader interface {
	FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]readmodel.ProfileProjection, error)
}

type legacyFallbackReaderAdapter struct {
	legacy legacyFallbackReader
}

// FallbackStoreReader is the readmodel-shaped relay-fallback surface (satisfied
// by the relaylookup client). AdaptFallbackReader wraps it into the query-shaped
// FallbackReader consumed by the Service.
type FallbackStoreReader = legacyFallbackReader

// AdaptFallbackReader wraps a readmodel-shaped relay fallback reader into the
// query-shaped FallbackReader. Returns nil when no fallback is configured.
func AdaptFallbackReader(r FallbackStoreReader) FallbackReader {
	if r == nil {
		return nil
	}
	return legacyFallbackReaderAdapter{legacy: r}
}

func (a legacyFallbackReaderAdapter) FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return a.legacy.FetchEventsByIDs(ctx, ids)
}

func (a legacyFallbackReaderAdapter) FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	rows, err := a.legacy.FetchProfilesByPubkeys(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Profile, len(rows))
	for pubkey, row := range rows {
		out[pubkey] = profileFromStore(row)
	}
	return out, nil
}

type legacyFallbackProfilePersister interface {
	PersistFallbackProfile(ctx context.Context, pp readmodel.ProfileProjection) error
}

type legacyFallbackProfilePersisterAdapter struct {
	legacy legacyFallbackProfilePersister
}

func (a legacyFallbackProfilePersisterAdapter) PersistFallbackProfile(ctx context.Context, p Profile) error {
	return a.legacy.PersistFallbackProfile(ctx, readmodel.ProfileProjection{
		Pubkey:            p.Pubkey,
		MetadataEventID:   p.MetadataEventID,
		MetadataCreatedAt: p.MetadataCreatedAt,
		ProfileJSON:       p.ProfileJSON,
	})
}

type legacyFallbackEventPersister interface {
	PersistFallbackEvent(ctx context.Context, eventID string, raw json.RawMessage) error
}

type legacyFallbackEventPersisterAdapter struct {
	legacy legacyFallbackEventPersister
}

func (a legacyFallbackEventPersisterAdapter) PersistFallbackEvent(ctx context.Context, eventID string, raw json.RawMessage) error {
	return a.legacy.PersistFallbackEvent(ctx, eventID, raw)
}

// AdaptFallbackEventPersister wraps a store-level event persister as a query-level one.
func AdaptFallbackEventPersister(persister any) FallbackEventPersister {
	if persister == nil {
		return nil
	}
	if adapted, ok := persister.(FallbackEventPersister); ok {
		return adapted
	}
	if legacy, ok := persister.(legacyFallbackEventPersister); ok {
		return legacyFallbackEventPersisterAdapter{legacy: legacy}
	}
	return nil
}

// AdaptFallbackProfilePersister wraps a store-level persister as a query-level one.
func AdaptFallbackProfilePersister(persister any) FallbackProfilePersister {
	if persister == nil {
		return nil
	}
	if adapted, ok := persister.(FallbackProfilePersister); ok {
		return adapted
	}
	if legacy, ok := persister.(legacyFallbackProfilePersister); ok {
		return legacyFallbackProfilePersisterAdapter{legacy: legacy}
	}
	return nil
}
