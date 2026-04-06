package api_primal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func BenchmarkWSGatewayDispatchCacheCallThreadView(b *testing.B) {
	replies := []json.RawMessage{
		json.RawMessage(`{"id":"reply_1","created_at":1}`),
		json.RawMessage(`{"id":"reply_2","created_at":2}`),
		json.RawMessage(`{"id":"reply_3","created_at":3}`),
	}
	gateway := NewWSGateway(fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"evt_root","created_at":1}`), nil
		},
		getEventAncestors: func(_ context.Context, _ string, _ int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{json.RawMessage(`{"id":"evt_ancestor","created_at":0}`)}, nil, nil
		},
		getEventRepliesFn: func(_ context.Context, _ string, _ int, _ *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			return replies, nil, nil
		},
	}, WSGatewayOptions{})

	kwargs := map[string]any{
		"event_id":  "evt_root",
		"limit":     20,
		"max_depth": 10,
		"offset":    0,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := gateway.dispatchCacheCall(context.Background(), "thread_view", kwargs)
		if err != nil {
			b.Fatalf("dispatchCacheCall thread_view: %v", err)
		}
		if len(out) == 0 {
			b.Fatal("expected non-empty thread_view payload")
		}
	}
}

func BenchmarkWSGatewayDispatchCacheCallUserInfos(b *testing.B) {
	gateway := NewWSGateway(fakeEventReader{
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			out := make(map[string]store.ProfileProjection, len(pubkeys))
			for _, pubkey := range pubkeys {
				out[pubkey] = store.ProfileProjection{
					Pubkey:            pubkey,
					MetadataEventID:   "meta_" + pubkey,
					MetadataCreatedAt: 1700000000,
					ProfileJSON:       json.RawMessage(`{"name":"bench"}`),
				}
			}
			return out, nil
		},
	}, WSGatewayOptions{})

	kwargs := map[string]any{
		"pubkeys": []any{
			"pk_1", "pk_2", "pk_3", "pk_4", "pk_5",
			"pk_6", "pk_7", "pk_8", "pk_9", "pk_10",
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := gateway.dispatchCacheCall(context.Background(), "user_infos", kwargs)
		if err != nil {
			b.Fatalf("dispatchCacheCall user_infos: %v", err)
		}
		if len(out) == 0 {
			b.Fatal("expected non-empty user_infos payload")
		}
	}
}
