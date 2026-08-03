package readmodel

import (
	"encoding/json"
	"testing"
)

func TestMergeEventCountsIntoRaw(t *testing.T) {
	raw := json.RawMessage(`{"id":"abc","content":"hello","pubkey":"pk"}`)
	out, err := MergeEventCountsIntoRaw(raw, EventCounts{
		ReplyCount:    4,
		ReactionCount: 7,
		RepostCount:   2,
		ZapCount:      1,
		ZapMSats:      21000,
		Consistency:   "eventual",
	})
	if err != nil {
		t.Fatalf("MergeEventCountsIntoRaw: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload["id"] != "abc" || payload["content"] != "hello" {
		t.Fatalf("lost original fields: %#v", payload)
	}
	if payload["reply_count"] != float64(4) || payload["reaction_count"] != float64(7) {
		t.Fatalf("unexpected top-level counts: %#v", payload)
	}
	if payload["repost_count"] != float64(2) || payload["zap_count"] != float64(1) || payload["zap_msats"] != float64(21000) {
		t.Fatalf("unexpected engagement fields: %#v", payload)
	}
	counts, ok := payload["counts"].(map[string]any)
	if !ok {
		t.Fatalf("expected counts object, got %#v", payload["counts"])
	}
	if counts["consistency"] != "eventual" {
		t.Fatalf("unexpected counts consistency: %#v", counts["consistency"])
	}
}
