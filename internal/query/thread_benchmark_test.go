package query

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
)

func BenchmarkServiceGetThreadWindow(b *testing.B) {
	pageOne := makeRepliesRange(1, 100)
	pageTwo := makeRepliesRange(101, 200)
	pageThree := makeRepliesRange(201, 260)

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
				return pageOne, &EventCursor{CreatedAt: 100, ID: "reply_100"}, nil
			case cursor.ID == "reply_100":
				return pageTwo, &EventCursor{CreatedAt: 200, ID: "reply_200"}, nil
			case cursor.ID == "reply_200":
				return pageThree, nil, nil
			default:
				return []json.RawMessage{}, nil, nil
			}
		},
	})

	req := ThreadWindowRequest{
		EventID:  "evt-thread",
		Limit:    50,
		MaxDepth: 100,
		Offset:   0,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := svc.GetThreadWindow(context.Background(), req)
		if err != nil {
			b.Fatalf("GetThreadWindow: %v", err)
		}
		if len(out.Replies) == 0 {
			b.Fatal("expected replies in thread window")
		}
	}
}

func BenchmarkServiceGetThreadWindowLargeThreadSmallLimit(b *testing.B) {
	var descCalls atomic.Int64
	svc := NewThreadService(fakeThreadReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"root"}`), nil
		},
		getEventAncestorsFn: func(_ context.Context, _ string, _ int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{}, []string{}, nil
		},
		getEventRepliesDescendingFn: func(_ context.Context, _ string, _ int, _ *EventCursor, _ int) ([]json.RawMessage, *EventCursor, error) {
			descCalls.Add(1)
			return []json.RawMessage{
				json.RawMessage(`{"id":"reply_1000000","created_at":1000000}`),
				json.RawMessage(`{"id":"reply_999999","created_at":999999}`),
				json.RawMessage(`{"id":"reply_999998","created_at":999998}`),
				json.RawMessage(`{"id":"reply_999997","created_at":999997}`),
				json.RawMessage(`{"id":"reply_999996","created_at":999996}`),
			}, &EventCursor{CreatedAt: 999996, ID: "reply_999996"}, nil
		},
	})
	req := ThreadWindowRequest{
		EventID: "evt-large-thread",
		Limit:   5,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := svc.GetThreadWindow(context.Background(), req)
		if err != nil {
			b.Fatalf("GetThreadWindow: %v", err)
		}
		if len(out.Replies) != 5 {
			b.Fatalf("expected 5 replies, got %d", len(out.Replies))
		}
	}
	b.StopTimer()
	if descCalls.Load() != int64(b.N) {
		b.Fatalf("expected one descending call per op, got %d for %d ops", descCalls.Load(), b.N)
	}
}
