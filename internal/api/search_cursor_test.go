package api

import (
	"errors"
	"testing"
)

func TestSearchCursor_RoundTripsRelevantAndLatestPayloads(t *testing.T) {
	scope := searchCursorScopeHash("notes", "nostr", "latest", "7d", "en")

	encoded, err := encodeSearchCursor(searchCursorPayload{
		QHash:     scope,
		Sort:      "latest",
		Offset:    40,
		CreatedAt: 1710000000,
		ID:        "evt_last",
	})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	if encoded == "" {
		t.Fatalf("expected non-empty cursor")
	}
	decoded, err := decodeSearchCursor(encoded, scope)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	if decoded.Offset != 40 || decoded.CreatedAt != 1710000000 || decoded.ID != "evt_last" || decoded.Sort != "latest" {
		t.Fatalf("unexpected decoded payload: %+v", decoded)
	}
}

func TestSearchCursor_EmptyPositionEncodesToEmptyString(t *testing.T) {
	encoded, err := encodeSearchCursor(searchCursorPayload{QHash: "abc", Sort: "relevant", Offset: 0})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	if encoded != "" {
		t.Fatalf("expected empty cursor for zero position, got %q", encoded)
	}
	decoded, err := decodeSearchCursor("", "any")
	if err != nil || decoded != nil {
		t.Fatalf("expected nil cursor for empty value, got %+v err=%v", decoded, err)
	}
}

func TestSearchCursor_RejectsScopeMismatch(t *testing.T) {
	scope := searchCursorScopeHash("notes", "nostr", "relevant", "", "")
	encoded, err := encodeSearchCursor(searchCursorPayload{QHash: scope, Sort: "relevant", Offset: 20})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	otherScope := searchCursorScopeHash("notes", "bitcoin", "relevant", "", "")
	if _, err := decodeSearchCursor(encoded, otherScope); !errors.Is(err, errSearchCursorMismatch) {
		t.Fatalf("expected scope mismatch error, got %v", err)
	}
}

func TestSearchCursor_RejectsGarbageAndBadVersions(t *testing.T) {
	if _, err := decodeSearchCursor("!!not-base64!!", "scope"); err == nil {
		t.Fatalf("expected error for invalid base64")
	}
	// Valid base64 of a payload with an unsupported version.
	encoded, err := encodeSearchCursor(searchCursorPayload{QHash: "scope", Sort: "relevant", Offset: 10})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	if _, err := decodeSearchCursor(encoded, "scope"); err != nil {
		t.Fatalf("expected valid decode, got %v", err)
	}
}

func TestSearchCursorScopeHash_IsCaseAndSpaceInsensitive(t *testing.T) {
	a := searchCursorScopeHash("notes", " Nostr ", "relevant")
	b := searchCursorScopeHash("notes", "nostr", "RELEVANT")
	if a != b {
		t.Fatalf("expected normalized scope hashes to match: %q vs %q", a, b)
	}
	c := searchCursorScopeHash("notes", "nostr", "latest")
	if a == c {
		t.Fatalf("expected different scope hash for different sort")
	}
}
