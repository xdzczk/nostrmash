package readmodel

import (
	"encoding/json"
	"testing"
)

func TestMergeTopZapIntoRaw(t *testing.T) {
	raw := json.RawMessage(`{"id":"abc","content":"hi"}`)
	out, err := MergeTopZapIntoRaw(raw, TopZapAd{
		Sats:         21000,
		SenderPubkey: "sender",
		DisplayName:  "Bob",
		Comment:      "nice",
	})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload["content"] != "hi" {
		t.Fatalf("content = %#v", payload["content"])
	}
	zap, ok := payload["top_zap"].(map[string]any)
	if !ok {
		t.Fatalf("top_zap = %#v", payload["top_zap"])
	}
	if zap["sats"] != float64(21000) || zap["display_name"] != "Bob" || zap["comment"] != "nice" {
		t.Fatalf("top_zap = %#v", zap)
	}
	if _, exists := zap["name"]; exists {
		t.Fatalf("empty name should be omitted, got %#v", zap)
	}
}

func TestLookupTopZapIgnoresCase(t *testing.T) {
	ads := map[string]TopZapAd{
		"abc": {Sats: 5, SenderPubkey: "pk"},
	}
	ad, ok := LookupTopZap(ads, "ABC")
	if !ok || ad.Sats != 5 {
		t.Fatalf("lookup = %#v %v", ad, ok)
	}
}
