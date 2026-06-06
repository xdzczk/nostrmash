package store

import (
	"encoding/json"
	"testing"
)

func TestEnrichZapRecord_PrefersDescriptionAmountAndText(t *testing.T) {
	record := json.RawMessage(`{
		"event_id":"zap_1",
		"sats":5,
		"event":{
			"id":"zap_1",
			"tags":[
				["amount","5000"],
				["description","{\"content\":\"great post\",\"tags\":[[\"amount\",\"21000\"]]}"]
			]
		}
	}`)
	enriched := enrichZapRecord(record)
	var out map[string]any
	if err := json.Unmarshal(enriched, &out); err != nil {
		t.Fatalf("decode enriched zap: %v", err)
	}
	if out["msats"] != float64(21000) {
		t.Fatalf("expected msats 21000, got %#v", out["msats"])
	}
	if out["sats"] != float64(21) {
		t.Fatalf("expected sats 21, got %#v", out["sats"])
	}
	if out["zap_text"] != "great post" {
		t.Fatalf("expected zap_text, got %#v", out["zap_text"])
	}
}

func TestEnrichZapRecord_FallsBackToProjectionSats(t *testing.T) {
	record := json.RawMessage(`{
		"event_id":"zap_2",
		"sats":42,
		"event":{"id":"zap_2","tags":[]}
	}`)
	enriched := enrichZapRecord(record)
	var out map[string]any
	if err := json.Unmarshal(enriched, &out); err != nil {
		t.Fatalf("decode enriched zap: %v", err)
	}
	if out["msats"] != float64(42000) {
		t.Fatalf("expected msats 42000, got %#v", out["msats"])
	}
}
