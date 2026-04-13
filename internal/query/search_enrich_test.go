package query

import (
	"encoding/json"
	"testing"
)

func TestExtractCandidatePubkeysFromEvents(t *testing.T) {
	events := []json.RawMessage{
		json.RawMessage(`{"pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tags":[["p","bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]]}`),
		json.RawMessage(`{"pubkey":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","tags":[]}`),
	}

	pubkeys := extractCandidatePubkeysFromEvents(events, 10)
	if len(pubkeys) != 3 {
		t.Fatalf("expected 3 pubkeys, got %d: %v", len(pubkeys), pubkeys)
	}
	if pubkeys[0] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Errorf("pubkeys[0] = %s", pubkeys[0])
	}
	if pubkeys[1] != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Errorf("pubkeys[1] = %s", pubkeys[1])
	}
	if pubkeys[2] != "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" {
		t.Errorf("pubkeys[2] = %s", pubkeys[2])
	}
}

func TestExtractCandidatePubkeysDeduplicates(t *testing.T) {
	pk := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	events := []json.RawMessage{
		json.RawMessage(`{"pubkey":"` + pk + `","tags":[["p","` + pk + `"]]}`),
	}
	pubkeys := extractCandidatePubkeysFromEvents(events, 10)
	if len(pubkeys) != 1 {
		t.Fatalf("expected 1 unique pubkey, got %d", len(pubkeys))
	}
}

func TestExtractCandidatePubkeysRespectsLimit(t *testing.T) {
	events := []json.RawMessage{
		json.RawMessage(`{"pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tags":[["p","bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]]}`),
		json.RawMessage(`{"pubkey":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","tags":[]}`),
	}
	pubkeys := extractCandidatePubkeysFromEvents(events, 2)
	if len(pubkeys) != 2 {
		t.Fatalf("expected 2 pubkeys (limited), got %d", len(pubkeys))
	}
}

func TestProfileMatchesTextQuery(t *testing.T) {
	profile := Profile{
		Pubkey:      "abc",
		ProfileJSON: json.RawMessage(`{"name":"gigi","display_name":"Gigi Sovereign","about":"bitcoin stuff","nip05":"gigi@example.com"}`),
	}

	if !profileMatchesTextQuery(profile, "gigi") {
		t.Error("expected match on name 'gigi'")
	}
	if !profileMatchesTextQuery(profile, "Gigi") {
		t.Error("expected case-insensitive match on 'Gigi'")
	}
	if !profileMatchesTextQuery(profile, "sovereign") {
		t.Error("expected match on display_name 'sovereign'")
	}
	if profileMatchesTextQuery(profile, "fiatjaf") {
		t.Error("expected no match on 'fiatjaf'")
	}
	if profileMatchesTextQuery(profile, "") {
		t.Error("expected no match on empty query")
	}
}
