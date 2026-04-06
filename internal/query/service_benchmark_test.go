package query

import (
	"context"
	"encoding/json"
	"fmt"
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

func BenchmarkServiceGetUserInfos(b *testing.B) {
	pubkeys := make([]string, 0, 240)
	for i := 0; i < 200; i++ {
		pubkeys = append(pubkeys, fmt.Sprintf("pk_%03d", i))
	}
	// Include duplicates and blanks to exercise normalization/ordering work.
	pubkeys = append(pubkeys, "pk_010", "pk_010", "", "   ", "pk_199")

	profiles := make(map[string]store.ProfileProjection, 200)
	for i := 0; i < 160; i++ {
		key := fmt.Sprintf("pk_%03d", i)
		profiles[key] = store.ProfileProjection{
			Pubkey:            key,
			MetadataEventID:   fmt.Sprintf("meta_%03d", i),
			MetadataCreatedAt: int64(1700000000 + i),
			ProfileJSON:       json.RawMessage(`{"name":"bench"}`),
		}
	}

	svc := NewProfileService(fakeProfileReader{
		getProfilesByPubkeysFn: func(_ context.Context, keys []string) (map[string]store.ProfileProjection, error) {
			out := make(map[string]store.ProfileProjection, len(keys))
			for _, key := range keys {
				if profile, ok := profiles[key]; ok {
					out[key] = profile
				}
			}
			return out, nil
		},
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := svc.GetProfiles(context.Background(), pubkeys)
		if err != nil {
			b.Fatalf("GetUserInfos: %v", err)
		}
		if len(out.Profiles) == 0 {
			b.Fatal("expected non-empty profile result")
		}
	}
}

func BenchmarkServiceGetEventBatch(b *testing.B) {
	ids := make([]string, 0, 240)
	for i := 0; i < 200; i++ {
		ids = append(ids, fmt.Sprintf("evt_%03d", i))
	}
	ids = append(ids, "evt_010", "evt_010", "evt_199")

	rows := make(map[string]json.RawMessage, 200)
	for i := 0; i < 180; i++ {
		id := fmt.Sprintf("evt_%03d", i)
		rows[id] = json.RawMessage(fmt.Sprintf(`{"id":"%s","kind":1}`, id))
	}

	svc := NewEventService(fakeEventReader{
		getEventBatchFn: func(_ context.Context, requested []string) (map[string]json.RawMessage, error) {
			out := make(map[string]json.RawMessage, len(requested))
			for _, id := range requested {
				if raw, ok := rows[id]; ok {
					out[id] = raw
				}
			}
			return out, nil
		},
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := svc.GetEvents(context.Background(), ids)
		if err != nil {
			b.Fatalf("GetEvents: %v", err)
		}
		if len(out) == 0 {
			b.Fatal("expected non-empty event batch result")
		}
	}
}

func makeRepliesRange(start, end int) []json.RawMessage {
	out := make([]json.RawMessage, 0, end-start+1)
	for i := start; i <= end; i++ {
		out = append(out, json.RawMessage(fmt.Sprintf(`{"id":"reply_%d","created_at":%d}`, i, i)))
	}
	return out
}
