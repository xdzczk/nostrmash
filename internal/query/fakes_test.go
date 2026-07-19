package query

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func mustNewService(tb testing.TB, reader Reader) Service {
	tb.Helper()
	svc, err := NewService(reader)
	if err != nil {
		tb.Fatalf("NewService: %v", err)
	}
	return svc
}

func mustNewServiceWithOptions(tb testing.TB, reader Reader, options ServiceOptions) Service {
	tb.Helper()
	svc, err := NewServiceWithOptions(reader, options)
	if err != nil {
		tb.Fatalf("NewServiceWithOptions: %v", err)
	}
	return svc
}

type fakeThreadReader struct {
	getEventRawByIDFn           func(ctx context.Context, id string) (json.RawMessage, error)
	getEventAncestorsFn         func(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
	getEventRepliesFn           func(ctx context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error)
	getEventRepliesDescendingFn func(ctx context.Context, eventID string, limit int, cursor *EventCursor, offset int) ([]json.RawMessage, *EventCursor, error)
}

func (f fakeThreadReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	if f.getEventRawByIDFn != nil {
		return f.getEventRawByIDFn(ctx, id)
	}
	return json.RawMessage(`{}`), nil
}

func (f fakeThreadReader) GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error) {
	if f.getEventRepliesFn != nil {
		return f.getEventRepliesFn(ctx, eventID, limit, cursor)
	}
	return []json.RawMessage{}, nil, nil
}

func (f fakeThreadReader) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
	if f.getEventAncestorsFn != nil {
		return f.getEventAncestorsFn(ctx, eventID, maxDepth)
	}
	return []json.RawMessage{}, []string{}, nil
}

func (f fakeThreadReader) GetEventRepliesDescending(ctx context.Context, eventID string, limit int, cursor *EventCursor, offset int) ([]json.RawMessage, *EventCursor, error) {
	if f.getEventRepliesDescendingFn != nil {
		return f.getEventRepliesDescendingFn(ctx, eventID, limit, cursor, offset)
	}
	return nil, nil, unsupportedCapabilityError("thread descending replies")
}

type fakeProfileReader struct {
	getProfilesByPubkeysFn  func(ctx context.Context, pubkeys []string) (map[string]Profile, error)
	getProfilePublicStatsFn func(ctx context.Context, pubkey string) (ProfilePublicStats, error)
}

func (f fakeProfileReader) GetProfileByPubkey(context.Context, string) (Profile, error) {
	return Profile{}, nil
}

func (f fakeProfileReader) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	if f.getProfilesByPubkeysFn != nil {
		return f.getProfilesByPubkeysFn(ctx, pubkeys)
	}
	return map[string]Profile{}, nil
}

func (f fakeProfileReader) GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (ProfilePublicStats, error) {
	if f.getProfilePublicStatsFn != nil {
		return f.getProfilePublicStatsFn(ctx, pubkey)
	}
	return ProfilePublicStats{Pubkey: pubkey}, nil
}

type fakeEventReader struct {
	getEventByIDFn   func(ctx context.Context, id string) (json.RawMessage, error)
	getEventBatchFn  func(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	getEventCountsFn func(ctx context.Context, eventID string) (EventCounts, error)
}

func (f fakeEventReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	if f.getEventByIDFn != nil {
		return f.getEventByIDFn(ctx, id)
	}
	return json.RawMessage(`{}`), nil
}

func (f fakeEventReader) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.getEventBatchFn != nil {
		return f.getEventBatchFn(ctx, ids)
	}
	return map[string]json.RawMessage{}, nil
}

func (f fakeEventReader) GetEventCounts(ctx context.Context, eventID string) (EventCounts, error) {
	if f.getEventCountsFn != nil {
		return f.getEventCountsFn(ctx, eventID)
	}
	return EventCounts{}, nil
}

func makeRepliesRange(start, end int) []json.RawMessage {
	out := make([]json.RawMessage, 0, end-start+1)
	for i := start; i <= end; i++ {
		out = append(out, json.RawMessage(fmt.Sprintf(`{"id":"reply_%d","created_at":%d}`, i, i)))
	}
	return out
}

type fakeFallbackReader struct {
	fetchEventsByIDsFn       func(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	fetchProfilesByPubkeysFn func(ctx context.Context, pubkeys []string) (map[string]Profile, error)
}

func (f fakeFallbackReader) FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.fetchEventsByIDsFn == nil {
		return map[string]json.RawMessage{}, nil
	}
	return f.fetchEventsByIDsFn(ctx, ids)
}

func (f fakeFallbackReader) FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	if f.fetchProfilesByPubkeysFn == nil {
		return map[string]Profile{}, nil
	}
	return f.fetchProfilesByPubkeysFn(ctx, pubkeys)
}
