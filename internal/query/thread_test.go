package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
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

func TestGetThreadWindowSmallThread(t *testing.T) {
	t.Parallel()
	svc := NewThreadService(fakeThreadReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"root"}`), nil
		},
		getEventAncestorsFn: func(_ context.Context, _ string, _ int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{}, []string{}, nil
		},
		getEventRepliesDescendingFn: func(_ context.Context, _ string, limit int, cursor *EventCursor, offset int) ([]json.RawMessage, *EventCursor, error) {
			if limit != 2 || cursor != nil || offset != 0 {
				t.Fatalf("unexpected request limit=%d cursor=%#v offset=%d", limit, cursor, offset)
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"reply_4","created_at":4}`),
				json.RawMessage(`{"id":"reply_3","created_at":3}`),
			}, &EventCursor{CreatedAt: 3, ID: "reply_3"}, nil
		},
	})

	out, err := svc.GetThreadWindow(context.Background(), ThreadWindowRequest{
		EventID:  "evt-thread",
		Limit:    2,
		MaxDepth: 10,
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
	if first.ID != "reply_4" {
		t.Fatalf("expected first reply_4, got %s", first.ID)
	}
	if out.NextCursor == nil || out.NextCursor.ID != "reply_3" {
		t.Fatalf("unexpected next cursor: %#v", out.NextCursor)
	}
}

func TestGetThreadWindowMultiPageThreadFallback(t *testing.T) {
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
				return makeRepliesRange(1, 100), &EventCursor{CreatedAt: 100, ID: "reply_100"}, nil
			case cursor.ID == "reply_100":
				return makeRepliesRange(101, 200), &EventCursor{CreatedAt: 200, ID: "reply_200"}, nil
			case cursor.ID == "reply_200":
				return makeRepliesRange(201, 240), nil, nil
			default:
				return nil, nil, fmt.Errorf("unexpected cursor %v", cursor)
			}
		},
	})
	out, err := svc.GetThreadWindow(context.Background(), ThreadWindowRequest{
		EventID: "evt-thread",
		Limit:   3,
		Offset:  2,
	})
	if err != nil {
		t.Fatalf("GetThreadWindow returned error: %v", err)
	}
	gotIDs := extractReplyIDs(t, out.Replies)
	wantIDs := []string{"reply_238", "reply_237", "reply_236"}
	if strings.Join(gotIDs, ",") != strings.Join(wantIDs, ",") {
		t.Fatalf("unexpected ids: got=%v want=%v", gotIDs, wantIDs)
	}
	if out.NextCursor == nil || out.NextCursor.ID != "reply_236" {
		t.Fatalf("unexpected next cursor: %#v", out.NextCursor)
	}
}

func TestGetThreadWindowRepeatedCursorDefense(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	svc := NewThreadService(fakeThreadReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"root"}`), nil
		},
		getEventAncestorsFn: func(_ context.Context, _ string, _ int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{}, []string{}, nil
		},
		getEventRepliesFn: func(_ context.Context, _ string, _ int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error) {
			calls.Add(1)
			switch {
			case cursor == nil:
				return makeRepliesRange(1, 100), &EventCursor{CreatedAt: 100, ID: "reply_100"}, nil
			case cursor.ID == "reply_100":
				return makeRepliesRange(101, 200), &EventCursor{CreatedAt: 100, ID: "reply_100"}, nil
			default:
				return []json.RawMessage{}, nil, nil
			}
		},
	})
	out, err := svc.GetThreadWindow(context.Background(), ThreadWindowRequest{
		EventID: "evt-thread",
		Limit:   2,
		Offset:  0,
	})
	if err != nil {
		t.Fatalf("GetThreadWindow returned error: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 calls with repeated cursor defense, got %d", calls.Load())
	}
	gotIDs := extractReplyIDs(t, out.Replies)
	wantIDs := []string{"reply_200", "reply_199"}
	if strings.Join(gotIDs, ",") != strings.Join(wantIDs, ",") {
		t.Fatalf("unexpected ids: got=%v want=%v", gotIDs, wantIDs)
	}
}

func TestGetThreadWindowLargeThreadSmallLimitUsesDescendingReader(t *testing.T) {
	t.Parallel()
	var ascCalls atomic.Int32
	var descCalls atomic.Int32
	svc := NewThreadService(fakeThreadReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"root"}`), nil
		},
		getEventAncestorsFn: func(_ context.Context, _ string, _ int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{}, []string{}, nil
		},
		getEventRepliesFn: func(_ context.Context, _ string, _ int, _ *EventCursor) ([]json.RawMessage, *EventCursor, error) {
			ascCalls.Add(1)
			return makeRepliesRange(1, 100), nil, nil
		},
		getEventRepliesDescendingFn: func(_ context.Context, _ string, limit int, cursor *EventCursor, offset int) ([]json.RawMessage, *EventCursor, error) {
			descCalls.Add(1)
			if cursor != nil || offset != 0 || limit != 5 {
				t.Fatalf("unexpected descending call params limit=%d cursor=%#v offset=%d", limit, cursor, offset)
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"reply_10000","created_at":10000}`),
				json.RawMessage(`{"id":"reply_9999","created_at":9999}`),
				json.RawMessage(`{"id":"reply_9998","created_at":9998}`),
				json.RawMessage(`{"id":"reply_9997","created_at":9997}`),
				json.RawMessage(`{"id":"reply_9996","created_at":9996}`),
			}, &EventCursor{CreatedAt: 9996, ID: "reply_9996"}, nil
		},
	})
	out, err := svc.GetThreadWindow(context.Background(), ThreadWindowRequest{
		EventID: "evt-thread",
		Limit:   5,
	})
	if err != nil {
		t.Fatalf("GetThreadWindow returned error: %v", err)
	}
	if ascCalls.Load() != 0 {
		t.Fatalf("expected no ascending scan calls, got %d", ascCalls.Load())
	}
	if descCalls.Load() != 1 {
		t.Fatalf("expected one descending call, got %d", descCalls.Load())
	}
	gotIDs := extractReplyIDs(t, out.Replies)
	wantIDs := []string{"reply_10000", "reply_9999", "reply_9998", "reply_9997", "reply_9996"}
	if strings.Join(gotIDs, ",") != strings.Join(wantIDs, ",") {
		t.Fatalf("unexpected ids: got=%v want=%v", gotIDs, wantIDs)
	}
}

func TestGetThreadWindowOffsetAndCursorCombination(t *testing.T) {
	t.Parallel()
	svc := NewThreadService(fakeThreadReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"root"}`), nil
		},
		getEventAncestorsFn: func(_ context.Context, _ string, _ int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{}, []string{}, nil
		},
		getEventRepliesDescendingFn: func(_ context.Context, _ string, limit int, cursor *EventCursor, offset int) ([]json.RawMessage, *EventCursor, error) {
			if cursor == nil || cursor.ID != "reply_50" {
				t.Fatalf("expected cursor reply_50, got %#v", cursor)
			}
			// Offset must be ignored when cursor is present to preserve existing behavior.
			if offset != 7 {
				t.Fatalf("expected forwarded offset value, got %d", offset)
			}
			if limit != 2 {
				t.Fatalf("unexpected limit: %d", limit)
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"reply_49","created_at":49}`),
				json.RawMessage(`{"id":"reply_48","created_at":48}`),
			}, &EventCursor{CreatedAt: 48, ID: "reply_48"}, nil
		},
	})
	out, err := svc.GetThreadWindow(context.Background(), ThreadWindowRequest{
		EventID: "evt-thread",
		Limit:   2,
		Cursor:  &EventCursor{CreatedAt: 50, ID: "reply_50"},
		Offset:  7,
	})
	if err != nil {
		t.Fatalf("GetThreadWindow returned error: %v", err)
	}
	gotIDs := extractReplyIDs(t, out.Replies)
	wantIDs := []string{"reply_49", "reply_48"}
	if strings.Join(gotIDs, ",") != strings.Join(wantIDs, ",") {
		t.Fatalf("unexpected ids: got=%v want=%v", gotIDs, wantIDs)
	}
	if out.NextCursor == nil || out.NextCursor.ID != "reply_48" {
		t.Fatalf("unexpected next cursor: %#v", out.NextCursor)
	}
}

func extractReplyIDs(t *testing.T, raws []json.RawMessage) []string {
	t.Helper()
	ids := make([]string, 0, len(raws))
	for _, raw := range raws {
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("decode reply: %v", err)
		}
		ids = append(ids, payload.ID)
	}
	return ids
}
