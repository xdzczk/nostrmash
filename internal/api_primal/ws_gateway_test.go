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

func TestWSGateway_EnforcesSubscriptionLimit(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{}, WSGatewayOptions{MaxSubscriptions: 1})
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

	if err := conn.WriteJSON([]any{"REQ", "sub1", map[string]any{"cache": []any{"nope"}}}); err != nil {
		t.Fatalf("write req 1: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil { // notice for unknown request
		t.Fatalf("read req1 frame: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil { // eose
		t.Fatalf("read req1 eose: %v", err)
	}

	if err := conn.WriteJSON([]any{"REQ", "sub2", map[string]any{"cache": []any{"events", map[string]any{"event_ids": []any{"evt_1"}}}}}); err != nil {
		t.Fatalf("write req 2: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read req2 frame: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if len(frame) < 3 || frame[0] != "NOTICE" || frame[2] != "too many subscriptions" {
		t.Fatalf("unexpected frame: %s", string(raw))
	}
}

func TestWSGateway_EnforcesRateLimit(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{}, WSGatewayOptions{MaxReqPerMinute: 1})
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

	if err := conn.WriteJSON([]any{"REQ", "sub1", map[string]any{"cache": []any{"nope"}}}); err != nil {
		t.Fatalf("write req1: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 notice: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 eose: %v", err)
	}

	if err := conn.WriteJSON([]any{"REQ", "sub2", map[string]any{"cache": []any{"nope"}}}); err != nil {
		t.Fatalf("write req2: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read req2 frame: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if len(frame) < 3 || frame[0] != "NOTICE" || frame[2] != "rate limit exceeded" {
		t.Fatalf("unexpected frame: %s", string(raw))
	}
}

func TestWSGateway_OriginPolicy(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{}, WSGatewayOptions{
		AllowedOrigins: []string{"https://allowed.example"},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"

	header := http.Header{}
	header.Set("Origin", "https://blocked.example")
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err == nil {
		t.Fatal("expected websocket dial to fail for blocked origin")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("unexpected status code: got=%d want=%d", status, http.StatusForbidden)
	}

	header = http.Header{}
	header.Set("Origin", "https://allowed.example")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("expected allowed origin dial success: %v", err)
	}
	_ = conn.Close()
}

func TestPrimalBatchUserInfosEndpoint(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				pubkeys[0]: {
					Pubkey:            pubkeys[0],
					MetadataEventID:   "evt_meta",
					MetadataCreatedAt: 1700000001,
					ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
				},
			}, nil
		},
	}, 10)
	req := httptest.NewRequest(http.MethodPost, "/primal/v1/user_infos", strings.NewReader(`{"pubkeys":["pk1","pk2"]}`))
	rec := httptest.NewRecorder()
	handlers.BatchGetUserInfos(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Profiles       []any    `json:"profiles"`
		MissingPubkeys []string `json:"missing_pubkeys"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Profiles) != 1 || len(resp.MissingPubkeys) != 1 || resp.MissingPubkeys[0] != "pk2" {
		t.Fatalf("unexpected response: %#v", resp)
	}
}
