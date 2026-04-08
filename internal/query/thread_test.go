package query

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

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
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error) {
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
		getEventRepliesFn: func(_ context.Context, _ string, _ int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error) {
			switch {
			case cursor == nil:
				return []json.RawMessage{
					json.RawMessage(`{"id":"reply_1","created_at":1}`),
					json.RawMessage(`{"id":"reply_2","created_at":2}`),
				}, &EventCursor{CreatedAt: 2, ID: "reply_2"}, nil
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
