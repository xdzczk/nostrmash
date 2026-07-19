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
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestWSGateway_ParametrizedReplaceableEventsContract(t *testing.T) {
	gateway := mustNewWSGateway(t, fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case pubkey == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" && kind == 30023 && dTag == "read-a":
				return json.RawMessage(`{"id":"pre_evt_a","kind":30023,"pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","created_at":30}`), nil
			case pubkey == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" && kind == 30023 && dTag == "read-b":
				return json.RawMessage(`{"id":"pre_evt_b","kind":30023,"pubkey":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","created_at":20}`), nil
			default:
				return nil, store.ErrNotFound
			}
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

	requestPath := filepath.Join("testdata", "ws_contracts", "parametrized_replaceable_events", "success", "request.json")
	requestRaw, err := os.ReadFile(requestPath)
	if err != nil {
		t.Fatalf("read parametrized_replaceable_events request: %v", err)
	}
	var requestPayload []any
	if err := json.Unmarshal(requestRaw, &requestPayload); err != nil {
		t.Fatalf("decode parametrized_replaceable_events request: %v", err)
	}
	if err := conn.WriteJSON(requestPayload); err != nil {
		t.Fatalf("write parametrized_replaceable_events request: %v", err)
	}
	frames := make([]any, 0, 4)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read parametrized_replaceable_events frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode parametrized_replaceable_events frame: %v", err)
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
	goldenPath := filepath.Join("testdata", "ws_contracts", "parametrized_replaceable_events", "success", "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}
