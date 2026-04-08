package query

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

type legacyReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventWithProvenance(ctx context.Context, id string) (store.EventWithProvenance, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error)
	GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error)
	GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error)
	GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error)
	GetEventReplies(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
	ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error)
	GetContactListByPubkey(ctx context.Context, pubkey string) (store.ContactListProjection, error)
	GetRelayListByPubkey(ctx context.Context, pubkey string) (store.RelayListProjection, error)
	SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error)
	SearchProfiles(ctx context.Context, query string, limit int) ([]store.ProfileProjection, error)
	GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
	GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
}

type legacyReaderAdapter struct {
	legacy legacyReader
}

type legacyDescendingThreadWindowReader interface {
	GetEventRepliesDescending(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor, offset int) ([]json.RawMessage, *store.EventOrderCursor, error)
}

func adaptReader(reader any) Reader {
	if adapted, ok := reader.(Reader); ok {
		return adapted
	}
	if legacy, ok := reader.(legacyReader); ok {
		return legacyReaderAdapter{legacy: legacy}
	}
	panic(fmt.Sprintf("query: unsupported reader type %T", reader))
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
	FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error)
}

type legacyFallbackReaderAdapter struct {
	legacy legacyFallbackReader
}

func adaptFallbackReader(reader any) FallbackReader {
	if reader == nil {
		return nil
	}
	if adapted, ok := reader.(FallbackReader); ok {
		return adapted
	}
	if legacy, ok := reader.(legacyFallbackReader); ok {
		return legacyFallbackReaderAdapter{legacy: legacy}
	}
	panic(fmt.Sprintf("query: unsupported fallback reader type %T", reader))
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
