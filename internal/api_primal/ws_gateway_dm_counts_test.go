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

func TestWSGateway_DirectMessageCountContracts(t *testing.T) {
	const validPubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := mustNewWSGateway(t, fakeEventReader{
		getDMCountFn: func(_ context.Context, receiver string, sender string) (int64, error) {
			return 7, nil
		},
	}, WSGatewayOptions{})
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

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_count", map[string]any{"cache": []any{"directmsg_count", map[string]any{"pubkey": validPubkey}}}}); err != nil {
		t.Fatalf("write directmsg_count req: %v", err)
	}
	_, firstRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first directmsg_count frame: %v", err)
	}
	var firstFrame []any
	if err := json.Unmarshal(firstRaw, &firstFrame); err != nil {
		t.Fatalf("decode first directmsg_count frame: %v", err)
	}
	if len(firstFrame) < 2 || firstFrame[0] != "EOSE" || firstFrame[1] != "sub_dm_count" {
		t.Fatalf("directmsg_count first frame must be EOSE: %s", string(firstRaw))
	}
	if err := conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond)); err != nil {
		t.Fatalf("set directmsg_count read deadline: %v", err)
	}
	_, countRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read broadcast directmsg_count frame: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	var countFrame []any
	if err := json.Unmarshal(countRaw, &countFrame); err != nil {
		t.Fatalf("decode broadcast directmsg_count frame: %v", err)
	}
	if len(countFrame) < 3 || countFrame[0] != "EVENT" || countFrame[1] != "sub_dm_count" {
		t.Fatalf("unexpected broadcast directmsg_count envelope: %s", string(countRaw))
	}
	event, ok := countFrame[2].(map[string]any)
	if !ok {
		t.Fatalf("directmsg_count payload is not object: %s", string(countRaw))
	}
	if event["kind"] != float64(10000117) || event["cnt"] != float64(7) {
		t.Fatalf("unexpected directmsg_count payload: %s", string(countRaw))
	}
	if _, exists := event["pubkey"]; exists {
		t.Fatalf("directmsg_count must not expose pubkey in payload: %s", string(countRaw))
	}
	if _, exists := event["sender"]; exists {
		t.Fatalf("directmsg_count must not expose sender in payload: %s", string(countRaw))
	}
	if err := conn.WriteJSON([]any{"CLOSE", "sub_dm_count"}); err != nil {
		t.Fatalf("close live dm count sub: %v", err)
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_count_2", map[string]any{"cache": []any{"directmsg_count_2", map[string]any{"pubkey": validPubkey}}}}); err != nil {
		t.Fatalf("write directmsg_count_2 req: %v", err)
	}
	_, first2Raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read first directmsg_count_2 frame: %v", err)
	}
	var first2Frame []any
	if err := json.Unmarshal(first2Raw, &first2Frame); err != nil {
		t.Fatalf("decode first directmsg_count_2 frame: %v", err)
	}
	if len(first2Frame) < 2 || first2Frame[0] != "EOSE" || first2Frame[1] != "sub_dm_count_2" {
		t.Fatalf("directmsg_count_2 first frame must be EOSE: %s", string(first2Raw))
	}
	if err := conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond)); err != nil {
		t.Fatalf("set directmsg_count_2 read deadline: %v", err)
	}
	_, count2Raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read broadcast directmsg_count_2 frame: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	var count2Frame []any
	if err := json.Unmarshal(count2Raw, &count2Frame); err != nil {
		t.Fatalf("decode broadcast directmsg_count_2 frame: %v", err)
	}
	if len(count2Frame) < 3 || count2Frame[0] != "EVENT" || count2Frame[1] != "sub_dm_count_2" {
		t.Fatalf("unexpected broadcast directmsg_count_2 envelope: %s", string(count2Raw))
	}
	event2, ok := count2Frame[2].(map[string]any)
	if !ok {
		t.Fatalf("directmsg_count_2 payload is not object: %s", string(count2Raw))
	}
	if event2["kind"] != float64(10000134) || event2["content"] != "7" {
		t.Fatalf("unexpected directmsg_count_2 payload: %s", string(count2Raw))
	}
	if _, exists := event2["cnt"]; exists {
		t.Fatalf("directmsg_count_2 must not expose cnt top-level: %s", string(count2Raw))
	}
}

func TestWSGateway_DirectMessageCountLiveRejectsInvalidPubkey(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{
		getDMCountFn: func(_ context.Context, receiver string, sender string) (int64, error) {
			return 7, nil
		},
	}, WSGatewayOptions{})
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

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_count_invalid", map[string]any{
		"cache": []any{"directmsg_count", map[string]any{"pubkey": "not-a-valid-pubkey"}},
	}}); err != nil {
		t.Fatalf("write invalid directmsg_count req: %v", err)
	}

	_, noticeRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read notice frame: %v", err)
	}
	var notice []any
	if err := json.Unmarshal(noticeRaw, &notice); err != nil {
		t.Fatalf("decode notice frame: %v", err)
	}
	if len(notice) < 3 || notice[0] != "NOTICE" || notice[1] != "sub_dm_count_invalid" || notice[2] != "invalid pubkey" {
		t.Fatalf("unexpected notice frame: %s", string(noticeRaw))
	}

	_, eoseRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read eose frame: %v", err)
	}
	var eose []any
	if err := json.Unmarshal(eoseRaw, &eose); err != nil {
		t.Fatalf("decode eose frame: %v", err)
	}
	if len(eose) < 2 || eose[0] != "EOSE" || eose[1] != "sub_dm_count_invalid" {
		t.Fatalf("unexpected eose frame: %s", string(eoseRaw))
	}
}
