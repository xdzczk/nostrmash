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

func TestWSGateway_LongFormContentThreadViewContract(t *testing.T) {
	const author = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gateway := mustNewWSGateway(t, fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"evt_read_root","kind":30023,"pubkey":"` + author + `"}`), nil
		},
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"` + id + `","kind":30023,"pubkey":"` + author + `"}`), nil
		},
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			return []json.RawMessage{json.RawMessage(`{"id":"evt_parent","kind":1,"pubkey":"` + author + `"}`)}, []string{}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"reply_2","kind":1,"created_at":22,"pubkey":"` + author + `"}`),
				json.RawMessage(`{"id":"reply_1","kind":1,"created_at":11,"pubkey":"` + author + `"}`),
			}, nil, nil
		},
		getEventsByATagAndKindFn: func(_ context.Context, kind int, aTagValue string, limit int) ([]json.RawMessage, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"reply_a_only","kind":1,"created_at":33,"pubkey":"` + author + `"}`),
			}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				author: {Pubkey: author, MetadataEventID: "md_author"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_author": json.RawMessage(`{"id":"md_author","kind":0,"pubkey":"` + author + `"}`),
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

	if err := conn.WriteJSON([]any{"REQ", "sub_read_thread_contract", map[string]any{"cache": []any{"long_form_content_thread_view", map[string]any{
		"pubkey":     author,
		"kind":       30023,
		"identifier": "read-id",
		"limit":      2,
	}}}}); err != nil {
		t.Fatalf("write long_form_content_thread_view request: %v", err)
	}
	frames := make([]any, 0, 8)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read long_form_content_thread_view frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode long_form_content_thread_view frame: %v", err)
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
	goldenPath := filepath.Join("testdata", "ws_contracts", "long_form_content_thread_view", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}
