package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestWSGateway_GetHighlightsContract(t *testing.T) {
	const highlightAuthor = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := mustNewWSGateway(t, fakeEventReader{
		getHighlightsByEventFn: func(_ context.Context, eventID string, limit int) ([]json.RawMessage, error) {
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

	if err := conn.WriteJSON([]any{"REQ", "sub_highlights_contract", map[string]any{"cache": []any{"get_highlights", map[string]any{
		"event_id": "evt_target",
		"limit":    2,
	}}}}); err != nil {
		t.Fatalf("write get_highlights request: %v", err)
	}
	frames := make([]any, 0, 6)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read get_highlights frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode get_highlights frame: %v", err)
		}
		frames = append(frames, frame)
		if len(frame) > 0 && frame[0] == "EOSE" {
			break
		}
	}
	actualBody, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal actual ws frames: %v", err)
	}
	goldenPath := filepath.Join("testdata", "ws_contracts", "get_highlights", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}
