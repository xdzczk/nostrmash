package relaypublish

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestPublishCollectsOK(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil || len(frame) < 2 || frame[0] != "EVENT" {
			return
		}
		event, _ := frame[1].(map[string]any)
		id, _ := event["id"].(string)
		_ = conn.WriteJSON([]any{"OK", id, true, ""})
	}))
	defer server.Close()

	relayURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := New()
	results := client.Publish(context.Background(), []string{relayURL}, json.RawMessage(`{"id":"abc","kind":1}`))
	if len(results) != 1 {
		t.Fatalf("results: %#v", results)
	}
	if len(results[0].Reply) < 3 || results[0].Reply[2] != true {
		t.Fatalf("reply: %#v", results[0].Reply)
	}
}
