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

func TestWSGateway_SearchFilterlistContract(t *testing.T) {
	const viewer = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const muted = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			if kind == 10000 && dTag == "" {
				return json.RawMessage(`{"id":"mute_base","kind":10000,"tags":[["p","` + muted + `"]]}`), nil
			}
			return nil, store.ErrNotFound
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				viewer: {Pubkey: viewer, MetadataEventID: "md_viewer"},
			}, nil
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"md_viewer": json.RawMessage(`{"id":"md_viewer","kind":0,"pubkey":"` + viewer + `"}`),
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

	if err := conn.WriteJSON([]any{"REQ", "sub_filterlist_contract", map[string]any{"cache": []any{"search_filterlist", map[string]any{
		"pubkey":      muted,
		"user_pubkey": viewer,
	}}}}); err != nil {
		t.Fatalf("write search_filterlist request: %v", err)
	}
	frames := make([]any, 0, 4)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read search_filterlist frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode search_filterlist frame: %v", err)
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
	goldenPath := filepath.Join("testdata", "ws_contracts", "search_filterlist", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}
