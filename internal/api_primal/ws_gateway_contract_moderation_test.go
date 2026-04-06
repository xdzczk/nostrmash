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

func TestWSGateway_IsHiddenByContentModerationContract(t *testing.T) {
	const viewer = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const muted = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const allowed = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case kind == 10000 && dTag == "":
				return json.RawMessage(`{"id":"mute_base","kind":10000,"tags":[["p","` + muted + `"],["word","scam"]]}`), nil
			case kind == 30000 && dTag == "allowlist":
				return json.RawMessage(`{"id":"allowlist_evt","kind":30000,"tags":[["d","allowlist"],["p","` + allowed + `"]]}`), nil
			default:
				return nil, store.ErrNotFound
			}
		},
		isHiddenFn: func(_ context.Context, viewerPubkey string, eventID string) (bool, string, error) {
			switch eventID {
			case "evt_hidden":
				return true, "muted_pubkey:" + muted, nil
			case "evt_visible":
				return false, "", nil
			default:
				return false, "", store.ErrNotFound
			}
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

	if err := conn.WriteJSON([]any{"REQ", "sub_hidden_contract", map[string]any{"cache": []any{"is_hidden_by_content_moderation", map[string]any{
		"user_pubkey": viewer,
		"pubkeys":     []any{muted, allowed},
		"event_ids":   []any{"evt_hidden", "evt_visible"},
		"event_id":    "evt_hidden",
	}}}}); err != nil {
		t.Fatalf("write is_hidden_by_content_moderation request: %v", err)
	}
	frames := make([]any, 0, 3)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read is_hidden_by_content_moderation frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode is_hidden_by_content_moderation frame: %v", err)
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
	goldenPath := filepath.Join("testdata", "ws_contracts", "is_hidden_by_content_moderation", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}
