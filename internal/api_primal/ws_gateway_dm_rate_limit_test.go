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

func TestWSGateway_DirectMessagesRateLimitAndValidation(t *testing.T) {
	const validPubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const peerPubkey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := mustNewWSGateway(t, fakeEventReader{
		getDirectMsgsRangeFn: func(_ context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error) {
			if pubkey != validPubkey || peer != peerPubkey || limit != 1 {
				t.Fatalf("unexpected DM query args pubkey=%s peer=%s limit=%d", pubkey, peer, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"dm_evt_1","kind":4}`)}, nil
		},
	}, WSGatewayOptions{
		MaxReqPerMinute:   10,
		MaxDMReqPerMinute: 1,
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON([]any{"REQ", "dm_sub_1", map[string]any{"cache": []any{"get_directmsgs", map[string]any{"pubkey": validPubkey, "peer_pubkey": peerPubkey, "limit": 1}}}}); err != nil {
		t.Fatalf("write req1: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 event: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 range: %v", err)
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read req1 eose: %v", err)
	}

	if err := conn.WriteJSON([]any{"REQ", "dm_sub_2", map[string]any{"cache": []any{"get_directmsgs", map[string]any{"pubkey": validPubkey, "peer_pubkey": peerPubkey, "limit": 1}}}}); err != nil {
		t.Fatalf("write req2: %v", err)
	}
	_, rateLimitedRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read req2 notice: %v", err)
	}
	var rateLimited []any
	if err := json.Unmarshal(rateLimitedRaw, &rateLimited); err != nil {
		t.Fatalf("decode req2 notice: %v", err)
	}
	if len(rateLimited) < 3 || rateLimited[0] != "NOTICE" || rateLimited[2] != "dm rate limit exceeded" {
		t.Fatalf("unexpected rate-limited frame: %s", string(rateLimitedRaw))
	}

	conn.Close()
	var dialResp *http.Response
	conn, dialResp, err = websocket.DefaultDialer.Dial(wsURL, nil)
	if dialResp != nil {
		_ = dialResp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial ws for validation case: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteJSON([]any{"REQ", "bad_dm", map[string]any{"cache": []any{"get_directmsgs", map[string]any{"pubkey": "not_hex", "peer_pubkey": peerPubkey, "limit": 1}}}}); err != nil {
		t.Fatalf("write invalid dm req: %v", err)
	}
	_, invalidRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read invalid dm notice: %v", err)
	}
	var invalid []any
	if err := json.Unmarshal(invalidRaw, &invalid); err != nil {
		t.Fatalf("decode invalid dm notice: %v", err)
	}
	if len(invalid) < 3 || invalid[0] != "NOTICE" || invalid[2] != "invalid pubkey" {
		t.Fatalf("unexpected invalid dm frame: %s", string(invalidRaw))
	}
}
