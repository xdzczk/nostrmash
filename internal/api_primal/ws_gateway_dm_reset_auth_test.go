package api_primal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestWSGateway_DMResetRequiresSignedAuthEvent(t *testing.T) {
	const peerPubkey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
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

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_reset_bad", map[string]any{
		"cache": []any{"reset_directmsg_count", map[string]any{"peer_pubkey": peerPubkey}},
	}}); err != nil {
		t.Fatalf("write bad reset req: %v", err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read bad reset response: %v", err)
	}
	var frame []any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode bad reset response: %v", err)
	}
	if len(frame) < 3 || frame[0] != "NOTICE" {
		t.Fatalf("expected NOTICE frame, got %s", string(raw))
	}
}
