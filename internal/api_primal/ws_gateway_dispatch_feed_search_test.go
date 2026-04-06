package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestWSGateway_CacheUserMentions(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getRefsPubkeyFn: func(_ context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
			if targetPubkey != "pk_mentions" || limit != 1 {
				t.Fatalf("unexpected mentions args pubkey=%s limit=%d", targetPubkey, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_mention_1","kind":1}`)}, nil
		},
	}, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON([]any{"REQ", "sub_mentions", map[string]any{"cache": []any{"user_mentions", map[string]any{"pubkey": "pk_mentions", "limit": 1}}}}); err != nil {
		t.Fatalf("write mentions req: %v", err)
	}
	_, eventRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read mentions event: %v", err)
	}
	var eventFrame []any
	if err := json.Unmarshal(eventRaw, &eventFrame); err != nil {
		t.Fatalf("decode mentions event: %v", err)
	}
	if len(eventFrame) < 2 || eventFrame[0] != "EVENT" || eventFrame[1] != "sub_mentions" {
		t.Fatalf("unexpected mentions frame: %s", string(eventRaw))
	}
}

func TestWSGateway_CacheUserFollowersUsesFollowerProjection(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getFollowersFn: func(_ context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
			if targetPubkey != "pk_followed" || limit != 1 {
				t.Fatalf("unexpected followers args pubkey=%s limit=%d", targetPubkey, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"follower_pubkey":"alice","source_event_id":"contact_evt_1"}`)}, nil
		},
	}, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON([]any{"REQ", "sub_followers", map[string]any{"cache": []any{"user_followers", map[string]any{"pubkey": "pk_followed", "limit": 1}}}}); err != nil {
		t.Fatalf("write followers req: %v", err)
	}
	_, eventRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read followers event: %v", err)
	}
	var eventFrame []any
	if err := json.Unmarshal(eventRaw, &eventFrame); err != nil {
		t.Fatalf("decode followers event: %v", err)
	}
	if len(eventFrame) < 2 || eventFrame[0] != "EVENT" || eventFrame[1] != "sub_followers" {
		t.Fatalf("unexpected followers frame: %s", string(eventRaw))
	}
}

func TestWSGateway_SearchAndCacheSearchReturnSameShape(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		searchEventsFn: func(_ context.Context, query string, limit int) ([]json.RawMessage, error) {
			return []json.RawMessage{json.RawMessage(`{"id":"evt_search_1","kind":1}`)}, nil
		},
		searchProfilesFn: func(_ context.Context, query string, limit int) ([]store.ProfileProjection, error) {
			return []store.ProfileProjection{{
				Pubkey:            "pk_search",
				MetadataEventID:   "evt_meta",
				MetadataCreatedAt: 1,
				ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
			}}, nil
		},
	}, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON([]any{"REQ", "sub_top", map[string]any{"search": "hello", "limit": 5}}); err != nil {
		t.Fatalf("write top-level search req: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read first top-level search frame: %v", err)
	}
	topProfileRaw := make([]byte, 0)
	for i := 0; i < 2; i++ {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read top-level frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode top-level frame: %v", err)
		}
		if len(frame) > 2 && frame[0] == "EVENT" {
			topProfileRaw = raw
		}
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_cache", map[string]any{"cache": []any{"search", map[string]any{"query": "hello", "limit": 5}}}}); err != nil {
		t.Fatalf("write cache search req: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read first cache search frame: %v", err)
	}
	cacheProfileRaw := make([]byte, 0)
	for i := 0; i < 2; i++ {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read cache frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode cache frame: %v", err)
		}
		if len(frame) > 2 && frame[0] == "EVENT" {
			cacheProfileRaw = raw
		}
	}
	if len(topProfileRaw) == 0 || len(cacheProfileRaw) == 0 {
		t.Fatal("expected search profile payloads for both top-level and cache search")
	}
}
