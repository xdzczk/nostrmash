package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestWSGateway_OneShotRequestsDoNotExhaustSubscriptions(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{}, nil
		},
	}, WSGatewayOptions{MaxSubscriptions: 1})
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
	if len(frame) < 2 || frame[0] != "EOSE" || frame[1] != "sub2" {
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
