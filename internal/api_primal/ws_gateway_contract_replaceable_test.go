package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestWSGateway_ParameterizedReplaceableListContract(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{
		getParamListByIdentifierFn: func(_ context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
			if pubkey != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
				return []json.RawMessage{}, nil
			}
			if kind != 30000 || identifier != "custom-list" || limit != 2 {
				return []json.RawMessage{}, nil
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"pl_evt_2","kind":30000,"pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","created_at":20,"tags":[["d","custom-list"],["p","bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]]}`),
				json.RawMessage(`{"id":"pl_evt_1","kind":30000,"pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","created_at":10,"tags":[["d","custom-list"],["p","cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"]]}`),
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

	requestPath := filepath.Join("testdata", "ws_contracts", "parameterized_replaceable_list", "success", "request.json")
	requestRaw, err := os.ReadFile(requestPath)
	if err != nil {
		t.Fatalf("read parameterized_replaceable_list request: %v", err)
	}
	var requestPayload []any
	if err := json.Unmarshal(requestRaw, &requestPayload); err != nil {
		t.Fatalf("decode parameterized_replaceable_list request: %v", err)
	}
	if err := conn.WriteJSON(requestPayload); err != nil {
		t.Fatalf("write parameterized_replaceable_list request: %v", err)
	}
	frames := make([]any, 0, 4)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read parameterized_replaceable_list frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode parameterized_replaceable_list frame: %v", err)
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
	goldenPath := filepath.Join("testdata", "ws_contracts", "parameterized_replaceable_list", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}
