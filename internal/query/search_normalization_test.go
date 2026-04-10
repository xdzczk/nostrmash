package query

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/btcsuite/btcd/btcutil/bech32"
)

func TestNormalizeProfileSearchQuery_StripsPrefixes(t *testing.T) {
	out := normalizeProfileSearchQuery("  nostr:@fiatjaf ")
	if out.NormalizedQuery != "fiatjaf" {
		t.Fatalf("unexpected normalized query: %q", out.NormalizedQuery)
	}
}

func TestNormalizeProfileSearchQuery_DecodesNpubIdentifier(t *testing.T) {
	pubkey := "f6e7657f7c0c6b03d4de2f2648c64d13f53cf9ce9e840ff6f3f4f85f8b5c5f55"
	npub := mustEncodeNpub(t, pubkey)
	out := normalizeProfileSearchQuery(npub)
	if out.CanonicalIdentifier != pubkey {
		t.Fatalf("unexpected canonical identifier: got %q want %q", out.CanonicalIdentifier, pubkey)
	}
}

func TestCanonicalizeEventIdentifier_HexEventID(t *testing.T) {
	eventID := "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	out := canonicalizeEventIdentifier(eventID)
	if out.EventID != eventID {
		t.Fatalf("expected hex event id passthrough, got %q", out.EventID)
	}
}

func TestCanonicalizeEventIdentifier_Note1(t *testing.T) {
	eventID := "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	note := mustEncodeNote1(t, eventID)
	out := canonicalizeEventIdentifier(note)
	if out.EventID != eventID {
		t.Fatalf("expected decoded event id %q, got %q", eventID, out.EventID)
	}
}

func TestCanonicalizeEventIdentifier_Nevent1(t *testing.T) {
	eventID := "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	relayURL := "wss://relay.example.com"
	nevent := mustEncodeNevent1(t, eventID, relayURL)
	out := canonicalizeEventIdentifier(nevent)
	if out.EventID != eventID {
		t.Fatalf("expected decoded event id %q, got %q", eventID, out.EventID)
	}
	if len(out.RelayHints) != 1 || out.RelayHints[0] != relayURL {
		t.Fatalf("expected relay hint %q, got %v", relayURL, out.RelayHints)
	}
}

func TestCanonicalizeEventIdentifier_InvalidInput(t *testing.T) {
	for _, input := range []string{"", "fiatjaf", "npub1abc", "xyz123"} {
		out := canonicalizeEventIdentifier(input)
		if out.EventID != "" {
			t.Fatalf("expected empty event id for %q, got %q", input, out.EventID)
		}
	}
}

func mustEncodeNpub(t *testing.T, pubkey string) string {
	t.Helper()
	raw, err := hex.DecodeString(pubkey)
	if err != nil {
		t.Fatalf("decode pubkey: %v", err)
	}
	words, err := bech32.ConvertBits(raw, 8, 5, true)
	if err != nil {
		t.Fatalf("convert bits: %v", err)
	}
	npub, err := bech32.Encode("npub", words)
	if err != nil {
		t.Fatalf("encode npub: %v", err)
	}
	return npub
}

func mustEncodeNote1(t *testing.T, eventID string) string {
	t.Helper()
	raw, err := hex.DecodeString(eventID)
	if err != nil {
		t.Fatalf("decode event id: %v", err)
	}
	words, err := bech32.ConvertBits(raw, 8, 5, true)
	if err != nil {
		t.Fatalf("convert bits: %v", err)
	}
	note, err := bech32.Encode("note", words)
	if err != nil {
		t.Fatalf("encode note: %v", err)
	}
	return note
}

func mustEncodeNevent1(t *testing.T, eventID, relayURL string) string {
	t.Helper()
	idBytes, err := hex.DecodeString(eventID)
	if err != nil {
		t.Fatalf("decode event id: %v", err)
	}
	var tlv []byte
	tlv = append(tlv, 0, byte(len(idBytes)))
	tlv = append(tlv, idBytes...)
	relayBytes := []byte(relayURL)
	tlv = append(tlv, 1, byte(len(relayBytes)))
	tlv = append(tlv, relayBytes...)
	words, err := bech32.ConvertBits(tlv, 8, 5, true)
	if err != nil {
		t.Fatalf("convert bits: %v", err)
	}
	nevent, err := bech32.Encode("nevent", words)
	if err != nil {
		t.Fatalf("encode nevent: %v", err)
	}
	return nevent
}

// suppress unused import
var _ = binary.BigEndian
