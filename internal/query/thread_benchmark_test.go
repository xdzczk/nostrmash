package query

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
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
		getEventRepliesFn: func(_ context.Context, _ string, _ int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			switch {
			case cursor == nil:
				return pageOne, &store.EventOrderCursor{CreatedAt: 100, ID: "reply_100"}, nil
			case cursor.ID == "reply_100":
				return pageTwo, &store.EventOrderCursor{CreatedAt: 200, ID: "reply_200"}, nil
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
