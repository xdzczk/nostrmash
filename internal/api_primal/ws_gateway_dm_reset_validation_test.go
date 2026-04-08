package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWSGateway_DMResetRejectsFutureEventAndEmitsNoEventOnSuccess(t *testing.T) {
	const peerPubkey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := mustNewWSGateway(t, fakeEventReader{
		resetDMCountFn: func(_ context.Context, receiver string, sender string) error {
			return nil
		},
		resetDMUnreadFn: func(_ context.Context, pubkey string, peer string) error {
			return nil
		},
		resetDMCountsFn: func(_ context.Context, receiver string) error {
			return nil
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

	futureAuthEvent := buildSignedAuthEventAt(t, time.Now().Add(301*time.Second).Unix())
	if err := conn.WriteJSON([]any{"REQ", "sub_dm_reset_future", map[string]any{
		"cache": []any{"reset_directmsg_count", map[string]any{"peer_pubkey": peerPubkey, "event_from_user": futureAuthEvent}},
	}}); err != nil {
		t.Fatalf("write future reset req: %v", err)
	}
	_, futureRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read future reset response: %v", err)
	}
	var futureFrame []any
	if err := json.Unmarshal(futureRaw, &futureFrame); err != nil {
		t.Fatalf("decode future reset response: %v", err)
	}
	if len(futureFrame) < 3 || futureFrame[0] != "NOTICE" || futureFrame[2] != "event from the future" {
		t.Fatalf("unexpected future reset frame: %s", string(futureRaw))
	}
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read future reset eose: %v", err)
	}

	okAuthEvent := buildSignedAuthEvent(t)
	if err := conn.WriteJSON([]any{"REQ", "sub_dm_reset_ok", map[string]any{
		"cache": []any{"reset_directmsg_count", map[string]any{"peer_pubkey": peerPubkey, "event_from_user": okAuthEvent}},
	}}); err != nil {
		t.Fatalf("write successful reset req: %v", err)
	}
	_, successRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read successful reset response: %v", err)
	}
	var successFrame []any
	if err := json.Unmarshal(successRaw, &successFrame); err != nil {
		t.Fatalf("decode successful reset response: %v", err)
	}
	if len(successFrame) < 2 || successFrame[0] != "EOSE" || successFrame[1] != "sub_dm_reset_ok" {
		t.Fatalf("successful reset must emit only EOSE: %s", string(successRaw))
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_reset_all_ok", map[string]any{
		"cache": []any{"reset_directmsg_counts", map[string]any{"event_from_user": okAuthEvent}},
	}}); err != nil {
		t.Fatalf("write successful reset-all req: %v", err)
	}
	_, resetAllRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read successful reset-all response: %v", err)
	}
	var resetAllFrame []any
	if err := json.Unmarshal(resetAllRaw, &resetAllFrame); err != nil {
		t.Fatalf("decode successful reset-all response: %v", err)
	}
	if len(resetAllFrame) < 2 || resetAllFrame[0] != "EOSE" || resetAllFrame[1] != "sub_dm_reset_all_ok" {
		t.Fatalf("successful reset-all must emit only EOSE: %s", string(resetAllRaw))
	}
}
