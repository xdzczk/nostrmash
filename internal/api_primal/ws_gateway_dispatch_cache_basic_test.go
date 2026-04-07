package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestWSGateway_REQCacheEventsThenEOSE(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			out := map[string]json.RawMessage{}
			for _, id := range ids {
				if id == "evt_1" {
					out[id] = json.RawMessage(`{"id":"evt_1","kind":1}`)
				}
			}
			return out, nil
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

	if err := conn.WriteJSON([]any{"REQ", "sub1", map[string]any{"cache": []any{"events", map[string]any{"event_ids": []any{"evt_1", "evt_2"}}}}}); err != nil {
		t.Fatalf("write req: %v", err)
	}

	_, firstRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	_, secondRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read second frame: %v", err)
	}
	var first []any
	if err := json.Unmarshal(firstRaw, &first); err != nil {
		t.Fatalf("decode first frame: %v", err)
	}
	if len(first) < 3 || first[0] != "EVENT" || first[1] != "sub1" {
		t.Fatalf("unexpected first frame: %s", string(firstRaw))
	}
	var second []any
	if err := json.Unmarshal(secondRaw, &second); err != nil {
		t.Fatalf("decode second frame: %v", err)
	}
	if len(second) < 2 || second[0] != "EOSE" || second[1] != "sub1" {
		t.Fatalf("unexpected second frame: %s", string(secondRaw))
	}
}

func TestWSGateway_REQIDsUsesRelayFallbackOnLocalMiss(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{}, nil
		},
	}, WSGatewayOptions{
		QueryOptions: query.ServiceOptions{
			FallbackReader: primalFakeFallbackReader{
				fetchEventsByIDsFn: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
					return map[string]json.RawMessage{
						"evt_fallback": json.RawMessage(`{"id":"evt_fallback","kind":1}`),
					}, nil
				},
			},
		},
	})
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

	if err := conn.WriteJSON([]any{"REQ", "sub_fallback", map[string]any{"ids": []any{"evt_fallback"}}}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, firstRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	var first []any
	if err := json.Unmarshal(firstRaw, &first); err != nil {
		t.Fatalf("decode first frame: %v", err)
	}
	if len(first) < 3 || first[0] != "EVENT" || first[1] != "sub_fallback" {
		t.Fatalf("unexpected first frame: %s", string(firstRaw))
	}
	_, secondRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read second frame: %v", err)
	}
	var second []any
	if err := json.Unmarshal(secondRaw, &second); err != nil {
		t.Fatalf("decode second frame: %v", err)
	}
	if len(second) < 2 || second[0] != "EOSE" || second[1] != "sub_fallback" {
		t.Fatalf("unexpected second frame: %s", string(secondRaw))
	}
}

func TestWSGateway_UnknownCacheRequestReturnsNotice(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{}, WSGatewayOptions{})
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

	if err := conn.WriteJSON([]any{"REQ", "sub_unknown", map[string]any{"cache": []any{"nope"}}}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read notice frame: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if len(frame) < 3 || frame[0] != "NOTICE" || frame[1] != "sub_unknown" {
		t.Fatalf("unexpected frame: %s", string(raw))
	}
}

func TestWSGateway_CacheUserProfile(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getProfileByPubkey: func(_ context.Context, pubkey string) (store.ProfileProjection, error) {
			return store.ProfileProjection{
				Pubkey:            pubkey,
				MetadataEventID:   "evt_meta_1",
				MetadataCreatedAt: 1700000001,
				ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
			}, nil
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
	if err := conn.WriteJSON([]any{"REQ", "sub_profile", map[string]any{"cache": []any{"user_profile", map[string]any{"pubkey": "pk1"}}}}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, firstRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	var first []any
	if err := json.Unmarshal(firstRaw, &first); err != nil {
		t.Fatalf("decode first frame: %v", err)
	}
	if len(first) < 3 || first[0] != "EVENT" || first[1] != "sub_profile" {
		t.Fatalf("unexpected first frame: %s", string(firstRaw))
	}
}

func TestWSGateway_CacheUserProfileUsesRelayFallbackOnLocalMiss(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getProfileByPubkey: func(_ context.Context, pubkey string) (store.ProfileProjection, error) {
			return store.ProfileProjection{}, store.ErrNotFound
		},
	}, WSGatewayOptions{
		QueryOptions: query.ServiceOptions{
			FallbackReader: primalFakeFallbackReader{
				fetchProfilesByPubkeysFn: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
					return map[string]store.ProfileProjection{
						"pk1": {
							Pubkey:            "pk1",
							MetadataEventID:   "evt_meta_fallback",
							MetadataCreatedAt: 1700000002,
							ProfileJSON:       json.RawMessage(`{"name":"fallback"}`),
						},
					}, nil
				},
			},
		},
	})
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
	if err := conn.WriteJSON([]any{"REQ", "sub_profile_fallback", map[string]any{"cache": []any{"user_profile", map[string]any{"pubkey": "pk1"}}}}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, firstRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	var first []any
	if err := json.Unmarshal(firstRaw, &first); err != nil {
		t.Fatalf("decode first frame: %v", err)
	}
	if len(first) < 3 || first[0] != "EVENT" || first[1] != "sub_profile_fallback" {
		t.Fatalf("unexpected first frame: %s", string(firstRaw))
	}
	_, secondRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read second frame: %v", err)
	}
	var second []any
	if err := json.Unmarshal(secondRaw, &second); err != nil {
		t.Fatalf("decode second frame: %v", err)
	}
	if len(second) < 2 || second[0] != "EOSE" || second[1] != "sub_profile_fallback" {
		t.Fatalf("unexpected second frame: %s", string(secondRaw))
	}
}

func TestWSGateway_CacheUserInfosUsesRelayFallbackOnLocalMiss(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{}, nil
		},
	}, WSGatewayOptions{
		QueryOptions: query.ServiceOptions{
			FallbackReader: primalFakeFallbackReader{
				fetchProfilesByPubkeysFn: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
					return map[string]store.ProfileProjection{
						"pk1": {
							Pubkey:            "pk1",
							MetadataEventID:   "evt_meta_fallback",
							MetadataCreatedAt: 1700000003,
							ProfileJSON:       json.RawMessage(`{"name":"fallback_batch"}`),
						},
					}, nil
				},
			},
		},
	})
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
	if err := conn.WriteJSON([]any{"REQ", "sub_profile_batch_fallback", map[string]any{"cache": []any{"user_infos", map[string]any{"pubkeys": []any{"pk1", "pk2"}}}}}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, firstRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	var first []any
	if err := json.Unmarshal(firstRaw, &first); err != nil {
		t.Fatalf("decode first frame: %v", err)
	}
	if len(first) < 3 || first[0] != "EVENT" || first[1] != "sub_profile_batch_fallback" {
		t.Fatalf("unexpected first frame: %s", string(firstRaw))
	}
	_, secondRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read second frame: %v", err)
	}
	var second []any
	if err := json.Unmarshal(secondRaw, &second); err != nil {
		t.Fatalf("decode second frame: %v", err)
	}
	if len(second) < 2 || second[0] != "EOSE" || second[1] != "sub_profile_batch_fallback" {
		t.Fatalf("unexpected second frame: %s", string(secondRaw))
	}
}

func TestWSGateway_GetBookmarksReturnsSingleLatestEvent(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			if pubkey != "pk_bookmarks" || kind != 10003 || dTag != "" {
				t.Fatalf("unexpected bookmark args pubkey=%s kind=%d dTag=%q", pubkey, kind, dTag)
			}
			return json.RawMessage(`{"id":"bookmark_evt_latest","kind":10003,"pubkey":"pk_bookmarks"}`), nil
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
	if err := conn.WriteJSON([]any{"REQ", "sub_bookmarks", map[string]any{
		"cache": []any{"get_bookmarks", map[string]any{"pubkey": "pk_bookmarks", "limit": 50}},
	}}); err != nil {
		t.Fatalf("write bookmarks req: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	if len(events) != 1 {
		t.Fatalf("expected one bookmark event, got=%d", len(events))
	}
	if id := eventIDFromAny(events[0]); id != "bookmark_evt_latest" {
		t.Fatalf("unexpected bookmark id: got=%q", id)
	}
}

func TestWSGateway_GetHighlightsByTargetIncludesMetadataAndRange(t *testing.T) {
	const highlightAuthor = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := NewWSGateway(fakeEventReader{
		getHighlightsByEventFn: func(_ context.Context, eventID string, limit int) ([]json.RawMessage, error) {
			if eventID != "evt_target" || limit != 2 {
				t.Fatalf("unexpected highlights args event_id=%s limit=%d", eventID, limit)
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"hl_2","kind":9802,"created_at":20,"pubkey":"` + highlightAuthor + `","tags":[["e","evt_target"]]}`),
				json.RawMessage(`{"id":"hl_1","kind":9802,"created_at":10,"pubkey":"` + highlightAuthor + `","tags":[["e","evt_target"]]}`),
			}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				highlightAuthor: {Pubkey: highlightAuthor, MetadataEventID: "md_highlighter"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_highlighter": json.RawMessage(`{"id":"md_highlighter","kind":0,"pubkey":"` + highlightAuthor + `"}`),
			}, nil
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
	if err := conn.WriteJSON([]any{"REQ", "sub_highlights", map[string]any{
		"cache": []any{"get_highlights", map[string]any{"event_id": "evt_target", "limit": 2}},
	}}); err != nil {
		t.Fatalf("write highlights req: %v", err)
	}
	frames := readThreadStreamUntilEOSE(t, conn)
	if len(frames) != 4 {
		t.Fatalf("unexpected highlights frame count: got=%d want=4", len(frames))
	}
	if id := eventIDFromAny(frames[0]); id != "hl_2" {
		t.Fatalf("unexpected first highlight id: %q", id)
	}
	if id := eventIDFromAny(frames[1]); id != "hl_1" {
		t.Fatalf("unexpected second highlight id: %q", id)
	}
	if id := eventIDFromAny(frames[2]); id != "md_highlighter" {
		t.Fatalf("unexpected metadata id: %q", id)
	}
	rangeEvent, ok := frames[3].(map[string]any)
	if !ok || rangeEvent["kind"] != float64(10000113) {
		t.Fatalf("unexpected highlights range event: %#v", frames[3])
	}
	contentRaw, _ := rangeEvent["content"].(string)
	var content map[string]any
	if err := json.Unmarshal([]byte(contentRaw), &content); err != nil {
		t.Fatalf("decode highlights range content: %v", err)
	}
	if content["order_by"] != "created_at" || content["since"] != float64(10) || content["until"] != float64(20) {
		t.Fatalf("unexpected highlights range content: %#v", content)
	}
}
