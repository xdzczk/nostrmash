package query

import (
	"context"
	"encoding/json"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type readModelReader interface {
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

type readModelNotesSearchReader interface {
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

type readModelProfilesSearchReader interface {
	SearchProfilesWithOptions(
		ctx context.Context,
		query string,
		sort string,
		limit int,
		offset int,
	) ([]readmodel.ProfileProjection, error)
}

type readModelSearchSuggestionsReader interface {
	SuggestProfiles(ctx context.Context, query string, limit int) ([]readmodel.ProfileProjection, error)
	SuggestHashtags(ctx context.Context, query string, limit int) ([]readmodel.TrendingHashtag, error)
}

type readModelSearchDocumentsReader interface {
	SearchDocuments(ctx context.Context, query string, limit int) ([]readmodel.SearchDocumentProjection, error)
}

type readModelReaderAdapter struct {
	readModel readModelReader
}

type readModelDescendingThreadWindowReader interface {
	GetEventRepliesDescending(ctx context.Context, eventID string, limit int, cursor *readmodel.EventOrderCursor, offset int) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
}

func (a readModelReaderAdapter) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	return a.readModel.GetEventRawByID(ctx, id)
}

func (a readModelReaderAdapter) GetEventWithProvenance(ctx context.Context, id string) (EventWithProvenance, error) {
	row, err := a.readModel.GetEventWithProvenance(ctx, id)
	if err != nil {
		return EventWithProvenance{}, err
	}
	return eventWithProvenanceFromStore(row), nil
}

func (a readModelReaderAdapter) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return a.readModel.GetEventRawsByIDs(ctx, ids)
}

func (a readModelReaderAdapter) GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error) {
	return a.readModel.GetEventSeenOn(ctx, id)
}

func (a readModelReaderAdapter) GetProfileByPubkey(ctx context.Context, pubkey string) (Profile, error) {
	row, err := a.readModel.GetProfileByPubkey(ctx, pubkey)
	if err != nil {
		return Profile{}, err
	}
	return profileFromStore(row), nil
}

func (a readModelReaderAdapter) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	rows, err := a.readModel.GetProfilesByPubkeys(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Profile, len(rows))
	for pubkey, row := range rows {
		out[pubkey] = profileFromStore(row)
	}
	return out, nil
}

func (a readModelReaderAdapter) GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (ProfilePublicStats, error) {
	row, err := a.readModel.GetProfilePublicStatsByPubkey(ctx, pubkey)
	if err != nil {
		return ProfilePublicStats{}, err
	}
	return profilePublicStatsFromStore(row), nil
}

func (a readModelReaderAdapter) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return a.readModel.GetAuthorRecentEvents(ctx, pubkey, limit)
}

func (a readModelReaderAdapter) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return a.readModel.GetAuthorReplies(ctx, pubkey, limit)
}

func (a readModelReaderAdapter) GetEventCounts(ctx context.Context, eventID string) (EventCounts, error) {
	row, err := a.readModel.GetEventCounts(ctx, eventID)
	if err != nil {
		return EventCounts{}, err
	}
	return eventCountsFromStore(row), nil
}

func (a readModelReaderAdapter) GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error) {
	replies, next, err := a.readModel.GetEventReplies(ctx, eventID, limit, eventCursorToStore(cursor))
	if err != nil {
		return nil, nil, err
	}
	return replies, eventCursorFromStore(next), nil
}

func (a readModelReaderAdapter) GetEventRepliesDescending(ctx context.Context, eventID string, limit int, cursor *EventCursor, offset int) ([]json.RawMessage, *EventCursor, error) {
	descReader, ok := a.readModel.(readModelDescendingThreadWindowReader)
	if !ok {
		return nil, nil, unsupportedCapabilityError("thread descending replies")
	}
	replies, next, err := descReader.GetEventRepliesDescending(ctx, eventID, limit, eventCursorToStore(cursor), offset)
	if err != nil {
		return nil, nil, err
	}
	return replies, eventCursorFromStore(next), nil
}

func (a readModelReaderAdapter) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
	return a.readModel.GetEventAncestors(ctx, eventID, maxDepth)
}

func (a readModelReaderAdapter) ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	return a.readModel.ListRelayHealth(ctx)
}

func (a readModelReaderAdapter) GetContactListByPubkey(ctx context.Context, pubkey string) (ContactList, error) {
	row, err := a.readModel.GetContactListByPubkey(ctx, pubkey)
	if err != nil {
		return ContactList{}, err
	}
	return contactListFromStore(row), nil
}

func (a readModelReaderAdapter) GetRelayListByPubkey(ctx context.Context, pubkey string) (RelayList, error) {
	row, err := a.readModel.GetRelayListByPubkey(ctx, pubkey)
	if err != nil {
		return RelayList{}, err
	}
	return relayListFromStore(row), nil
}

func (a readModelReaderAdapter) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	return a.readModel.SearchEventsByContent(ctx, query, limit)
}

func (a readModelReaderAdapter) SearchProfiles(ctx context.Context, query string, limit int) ([]Profile, error) {
	rows, err := a.readModel.SearchProfiles(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Profile, 0, len(rows))
	for _, row := range rows {
		out = append(out, profileFromStore(row))
	}
	return out, nil
}

func (a readModelReaderAdapter) SearchNotes(
	ctx context.Context,
	query string,
	sort string,
	window *time.Duration,
	language string,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if advanced, ok := a.readModel.(readModelNotesSearchReader); ok {
		return advanced.SearchNotes(ctx, query, sort, window, language, limit, offset)
	}
	if sort == "relevant" && window == nil && offset == 0 && language == "" {
		return a.readModel.SearchEventsByContent(ctx, query, limit)
	}
	return nil, unsupportedCapabilityError("advanced notes search")
}

func (a readModelReaderAdapter) SearchProfilesWithOptions(
	ctx context.Context,
	query string,
	sort string,
	limit int,
	offset int,
) ([]Profile, error) {
	if advanced, ok := a.readModel.(readModelProfilesSearchReader); ok {
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

func (a readModelReaderAdapter) SuggestProfiles(ctx context.Context, query string, limit int) ([]Profile, error) {
	reader, ok := a.readModel.(readModelSearchSuggestionsReader)
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

func (a readModelReaderAdapter) SuggestHashtags(ctx context.Context, query string, limit int) ([]HashtagSuggestion, error) {
	reader, ok := a.readModel.(readModelSearchSuggestionsReader)
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

func (a readModelReaderAdapter) SearchDocuments(ctx context.Context, query string, limit int) ([]SearchDocument, error) {
	reader, ok := a.readModel.(readModelSearchDocumentsReader)
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

func (a readModelReaderAdapter) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	return a.readModel.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
}

func (a readModelReaderAdapter) GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	return a.readModel.GetEventsReferencingPubkey(ctx, targetPubkey, limit)
}

func (a readModelReaderAdapter) GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	return a.readModel.GetFollowersByPubkey(ctx, targetPubkey, limit)
}

type readModelFallbackReader interface {
	FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]readmodel.ProfileProjection, error)
}

type readModelFallbackReaderAdapter struct {
	readModel readModelFallbackReader
}

// FallbackStoreReader is the readmodel-shaped relay-fallback surface (satisfied
// by the relaylookup client). AdaptFallbackReader wraps it into the query-shaped
// FallbackReader consumed by the Service.
type FallbackStoreReader = readModelFallbackReader

// AdaptFallbackReader wraps a readmodel-shaped relay fallback reader into the
// query-shaped FallbackReader. Returns nil when no fallback is configured.
func AdaptFallbackReader(r FallbackStoreReader) FallbackReader {
	if r == nil {
		return nil
	}
	return readModelFallbackReaderAdapter{readModel: r}
}

func (a readModelFallbackReaderAdapter) FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return a.readModel.FetchEventsByIDs(ctx, ids)
}

func (a readModelFallbackReaderAdapter) FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	rows, err := a.readModel.FetchProfilesByPubkeys(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Profile, len(rows))
	for pubkey, row := range rows {
		out[pubkey] = profileFromStore(row)
	}
	return out, nil
}

type readModelFallbackProfilePersister interface {
	PersistFallbackProfile(ctx context.Context, pp readmodel.ProfileProjection) error
}

type readModelFallbackProfilePersisterAdapter struct {
	readModel readModelFallbackProfilePersister
}

func (a readModelFallbackProfilePersisterAdapter) PersistFallbackProfile(ctx context.Context, p Profile) error {
	return a.readModel.PersistFallbackProfile(ctx, readmodel.ProfileProjection{
		Pubkey:            p.Pubkey,
		MetadataEventID:   p.MetadataEventID,
		MetadataCreatedAt: p.MetadataCreatedAt,
		ProfileJSON:       p.ProfileJSON,
	})
}

type readModelFallbackEventPersister interface {
	PersistFallbackEvent(ctx context.Context, eventID string, raw json.RawMessage) error
}

type readModelFallbackEventPersisterAdapter struct {
	readModel readModelFallbackEventPersister
}

func (a readModelFallbackEventPersisterAdapter) PersistFallbackEvent(ctx context.Context, eventID string, raw json.RawMessage) error {
	return a.readModel.PersistFallbackEvent(ctx, eventID, raw)
}

// AdaptFallbackEventPersister wraps a store-level event persister as a query-level one.
func AdaptFallbackEventPersister(persister any) FallbackEventPersister {
	if persister == nil {
		return nil
	}
	if adapted, ok := persister.(FallbackEventPersister); ok {
		return adapted
	}
	if readModel, ok := persister.(readModelFallbackEventPersister); ok {
		return readModelFallbackEventPersisterAdapter{readModel: readModel}
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
	if readModel, ok := persister.(readModelFallbackProfilePersister); ok {
		return readModelFallbackProfilePersisterAdapter{readModel: readModel}
	}
	return nil
}
