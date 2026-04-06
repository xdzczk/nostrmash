package query

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

type fakeThreadReader struct {
	getEventRawByIDFn      func(ctx context.Context, id string) (json.RawMessage, error)
	getEventAncestorsFn    func(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
	getEventRepliesFn      func(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
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
	getEventByIDFn     func(ctx context.Context, id string) (json.RawMessage, error)
	getEventBatchFn    func(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	getEventCountsFn   func(ctx context.Context, eventID string) (store.EventCounts, error)
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

func TestGetThreadUsesRequestModel(t *testing.T) {
	t.Parallel()
	var gotID string
	var gotLimit int
	var gotDepth int

	svc := NewThreadService(fakeThreadReader{
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			gotID = id
			return json.RawMessage(`{"id":"root"}`), nil
		},
		getEventAncestorsFn: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			gotDepth = maxDepth
			return []json.RawMessage{json.RawMessage(`{"id":"ancestor"}`)}, []string{}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			gotLimit = limit
			return []json.RawMessage{json.RawMessage(`{"id":"reply"}`)}, nil, nil
		},
	})

	out, err := svc.GetThread(context.Background(), ThreadRequest{
		EventID:  "  ev-1  ",
		Limit:    25,
		MaxDepth: 40,
	})
	if err != nil {
		t.Fatalf("GetThread returned error: %v", err)
	}
	if gotID != "ev-1" {
		t.Fatalf("expected trimmed id ev-1, got %q", gotID)
	}
	if gotLimit != 25 {
		t.Fatalf("expected limit 25, got %d", gotLimit)
	}
	if gotDepth != 40 {
		t.Fatalf("expected max depth 40, got %d", gotDepth)
	}
	if out.Consistency != "eventual" {
		t.Fatalf("expected eventual consistency, got %q", out.Consistency)
	}
}

func TestGetThreadReturnsThreadEventNotFoundForMissingRoot(t *testing.T) {
	t.Parallel()
	svc := NewThreadService(fakeThreadReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	})
	_, err := svc.GetThread(context.Background(), ThreadRequest{EventID: "evt-404"})
	if !errors.Is(err, ErrThreadEventNotFound) {
		t.Fatalf("expected ErrThreadEventNotFound, got %v", err)
	}
}

func TestGetThreadWindowBuildsDescendingPage(t *testing.T) {
	t.Parallel()
	svc := NewThreadService(fakeThreadReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"root"}`), nil
		},
		getEventAncestorsFn: func(_ context.Context, _ string, _ int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{}, []string{}, nil
		},
		getEventRepliesFn: func(_ context.Context, _ string, _ int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			switch {
			case cursor == nil:
				return []json.RawMessage{
					json.RawMessage(`{"id":"reply_1","created_at":1}`),
					json.RawMessage(`{"id":"reply_2","created_at":2}`),
				}, &store.EventOrderCursor{CreatedAt: 2, ID: "reply_2"}, nil
			case cursor.ID == "reply_2":
				return []json.RawMessage{
					json.RawMessage(`{"id":"reply_3","created_at":3}`),
					json.RawMessage(`{"id":"reply_4","created_at":4}`),
				}, nil, nil
			default:
				return []json.RawMessage{}, nil, nil
			}
		},
	})

	out, err := svc.GetThreadWindow(context.Background(), ThreadWindowRequest{
		EventID:  "evt-thread",
		Limit:    2,
		MaxDepth: 10,
		Offset:   1,
	})
	if err != nil {
		t.Fatalf("GetThreadWindow returned error: %v", err)
	}
	if len(out.Replies) != 2 {
		t.Fatalf("expected 2 replies, got %d", len(out.Replies))
	}
	var first struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(out.Replies[0], &first); err != nil {
		t.Fatalf("decode first reply: %v", err)
	}
	if first.ID != "reply_3" {
		t.Fatalf("expected first reply_3, got %s", first.ID)
	}
	if out.NextCursor == nil || out.NextCursor.ID != "reply_2" {
		t.Fatalf("unexpected next cursor: %#v", out.NextCursor)
	}
}

func TestGetProfilesNormalizesInputOrder(t *testing.T) {
	t.Parallel()
	svc := NewProfileService(fakeProfileReader{
		getProfilesByPubkeysFn: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			if len(pubkeys) != 2 || pubkeys[0] != "pk1" || pubkeys[1] != "pk2" {
				t.Fatalf("unexpected normalized pubkeys: %#v", pubkeys)
			}
			return map[string]store.ProfileProjection{
				"pk1": {Pubkey: "pk1"},
			}, nil
		},
	})

	out, err := svc.GetProfiles(context.Background(), []string{" pk1 ", "", "pk2", "pk1"})
	if err != nil {
		t.Fatalf("GetProfiles returned error: %v", err)
	}
	if len(out.Profiles) != 1 || out.Profiles[0].Pubkey != "pk1" {
		t.Fatalf("unexpected profiles result: %#v", out.Profiles)
	}
	if len(out.MissingPubkeys) != 1 || out.MissingPubkeys[0] != "pk2" {
		t.Fatalf("unexpected missing pubkeys: %#v", out.MissingPubkeys)
	}
}

func TestGetEventActionCountsAliasesLegacyMethod(t *testing.T) {
	t.Parallel()
	svc := NewEventService(fakeEventReader{
		getEventCountsFn: func(_ context.Context, eventID string) (store.EventCounts, error) {
			if eventID != "event-1" {
				t.Fatalf("unexpected event id: %q", eventID)
			}
			return store.EventCounts{
				EventID:       "event-1",
				ReplyCount:    2,
				ReactionCount: 3,
				RepostCount:   4,
				Consistency:   "eventual",
			}, nil
		},
	})

	out, err := svc.GetEventActionCounts(context.Background(), "event-1")
	if err != nil {
		t.Fatalf("GetEventActionCounts returned error: %v", err)
	}
	if out.EventID != "event-1" || out.ReplyCount != 2 || out.ReactionCount != 3 || out.RepostCount != 4 {
		t.Fatalf("unexpected action counts: %#v", out)
	}
}
