package query

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xdzczk/nostrmash/internal/store"
)

type fakeThreadReader struct {
	getEventRawByIDFn   func(ctx context.Context, id string) (json.RawMessage, error)
	getEventAncestorsFn func(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
	getEventRepliesFn   func(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
}

func (f fakeThreadReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	if f.getEventRawByIDFn != nil {
		return f.getEventRawByIDFn(ctx, id)
	}
	return json.RawMessage(`{}`), nil
}

func (f fakeThreadReader) GetEventReplies(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
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

type fakeProfileReader struct {
	getProfilesByPubkeysFn func(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error)
}

func (f fakeProfileReader) GetProfileByPubkey(context.Context, string) (store.ProfileProjection, error) {
	return store.ProfileProjection{}, nil
}

func (f fakeProfileReader) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
	if f.getProfilesByPubkeysFn != nil {
		return f.getProfilesByPubkeysFn(ctx, pubkeys)
	}
	return map[string]store.ProfileProjection{}, nil
}

type fakeEventReader struct {
	getEventByIDFn   func(ctx context.Context, id string) (json.RawMessage, error)
	getEventBatchFn  func(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	getEventCountsFn func(ctx context.Context, eventID string) (store.EventCounts, error)
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

func (f fakeEventReader) GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error) {
	if f.getEventCountsFn != nil {
		return f.getEventCountsFn(ctx, eventID)
	}
	return store.EventCounts{}, nil
}

func makeRepliesRange(start, end int) []json.RawMessage {
	out := make([]json.RawMessage, 0, end-start+1)
	for i := start; i <= end; i++ {
		out = append(out, json.RawMessage(fmt.Sprintf(`{"id":"reply_%d","created_at":%d}`, i, i)))
	}
	return out
}
