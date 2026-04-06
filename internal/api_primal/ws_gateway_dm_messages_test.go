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

func TestWSGateway_GetDirectMsgsContractOrdering(t *testing.T) {
	const receiver = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const sender = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := NewWSGateway(fakeEventReader{
		getDirectMsgsRangeFn: func(_ context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"dm_2","kind":4,"created_at":20}`),
				json.RawMessage(`{"id":"dm_1","kind":4,"created_at":10}`),
			}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				receiver: {Pubkey: receiver, MetadataEventID: "md_receiver"},
				sender:   {Pubkey: sender, MetadataEventID: "md_sender"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_receiver": json.RawMessage(`{"id":"md_receiver","kind":0,"pubkey":"` + receiver + `"}`),
				"md_sender":   json.RawMessage(`{"id":"md_sender","kind":0,"pubkey":"` + sender + `"}`),
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

	if err := conn.WriteJSON([]any{"REQ", "sub_dm_msgs", map[string]any{"cache": []any{"get_directmsgs", map[string]any{"pubkey": receiver, "peer_pubkey": sender, "limit": 2}}}}); err != nil {
		t.Fatalf("write get_directmsgs req: %v", err)
	}

	frames := make([][]any, 0, 8)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
		frames = append(frames, frame)
	}
	if len(frames) != 5 {
		t.Fatalf("unexpected event count: got=%d want=5", len(frames))
	}
	if event, ok := frames[0][2].(map[string]any); !ok || event["id"] != "dm_2" {
		t.Fatalf("unexpected first message frame: %#v", frames[0])
	}
	if event, ok := frames[1][2].(map[string]any); !ok || event["id"] != "dm_1" {
		t.Fatalf("unexpected second message frame: %#v", frames[1])
	}
	if event, ok := frames[2][2].(map[string]any); !ok || event["id"] != "md_receiver" {
		t.Fatalf("unexpected receiver metadata frame: %#v", frames[2])
	}
	if event, ok := frames[3][2].(map[string]any); !ok || event["id"] != "md_sender" {
		t.Fatalf("unexpected sender metadata frame: %#v", frames[3])
	}
	rangeEvent, ok := frames[4][2].(map[string]any)
	if !ok || rangeEvent["kind"] != float64(10000113) {
		t.Fatalf("unexpected range frame: %#v", frames[4])
	}
	contentRaw, _ := rangeEvent["content"].(string)
	var rangeContent map[string]any
	if err := json.Unmarshal([]byte(contentRaw), &rangeContent); err != nil {
		t.Fatalf("decode range content: %v", err)
	}
	if rangeContent["order_by"] != "created_at" || rangeContent["since"] != float64(10) || rangeContent["until"] != float64(20) {
		t.Fatalf("unexpected range content: %#v", rangeContent)
	}
}
