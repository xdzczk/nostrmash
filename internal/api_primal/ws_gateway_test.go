package api_primal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/gorilla/websocket"
)

func readThreadStreamUntilEOSE(t *testing.T, conn *websocket.Conn) []any {
	t.Helper()
	events := make([]any, 0, 8)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read ws frame: %v", err)
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode ws frame: %v", err)
		}
		if len(frame) == 0 {
			continue
		}
		if frame[0] == "EOSE" {
			break
		}
		if frame[0] == "NOTICE" {
			t.Fatalf("unexpected notice frame: %s", string(raw))
		}
		if frame[0] != "EVENT" || len(frame) < 3 {
			t.Fatalf("unexpected ws frame: %s", string(raw))
		}
		events = append(events, frame[2])
	}
	if len(events) == 0 {
		t.Fatalf("no event payload received before EOSE")
	}
	return events
}

func extractThreadRangeNextCursor(t *testing.T, events []any) string {
	t.Helper()
	for _, raw := range events {
		event, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		kind, ok := event["kind"].(float64)
		if !ok || int(kind) != 10000113 {
			continue
		}
		content, _ := event["content"].(string)
		var payload map[string]any
		if err := json.Unmarshal([]byte(content), &payload); err != nil {
			t.Fatalf("decode thread range content: %v", err)
		}
		next, _ := payload["next_cursor"].(string)
		return next
	}
	t.Fatalf("range event not found in thread stream")
	return ""
}

func extractEventIDsFromThreadStream(events []any) map[string]bool {
	out := make(map[string]bool)
	for _, raw := range events {
		event, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, _ := event["id"].(string)
		id = strings.TrimSpace(id)
		if id != "" {
			out[id] = true
		}
	}
	return out
}

func extractOrderedReplyIDs(events []any) []string {
	out := make([]string, 0, len(events))
	for _, raw := range events {
		event, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, _ := event["id"].(string)
		id = strings.TrimSpace(id)
		if !strings.HasPrefix(id, "reply_") {
			continue
		}
		out = append(out, id)
	}
	return out
}

func keysFromBoolMap(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func eventIDFromAny(value any) string {
	event, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	id, _ := event["id"].(string)
	return strings.TrimSpace(id)
}

func indexOfEventID(events []any, id string) int {
	for i, value := range events {
		if eventIDFromAny(value) == id {
			return i
		}
	}
	return -1
}

func indexOfRangeEvent(events []any) int {
	for i, value := range events {
		event, ok := value.(map[string]any)
		if !ok {
			continue
		}
		kind, ok := event["kind"].(float64)
		if ok && int(kind) == 10000113 {
			return i
		}
	}
	return -1
}

func buildSignedAuthEvent(t *testing.T) map[string]any {
	return buildSignedAuthEventAt(t, time.Now().Unix())
}

func buildSignedAuthEventAt(t *testing.T, createdAt int64) map[string]any {
	t.Helper()
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new private key: %v", err)
	}
	pubkey := hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))
	kind := 27235
	tags := [][]string{{"t", "dm_reset"}}
	content := "reset dm counters"
	canonical, err := json.Marshal([]any{0, pubkey, createdAt, kind, tags, content})
	if err != nil {
		t.Fatalf("marshal canonical auth event: %v", err)
	}
	sum := sha256.Sum256(canonical)
	id := hex.EncodeToString(sum[:])
	sig, err := schnorr.Sign(priv, sum[:])
	if err != nil {
		t.Fatalf("sign auth event: %v", err)
	}
	return map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       tags,
		"content":    content,
		"sig":        hex.EncodeToString(sig.Serialize()),
	}
}
