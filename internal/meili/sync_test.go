package meili

import (
	"strings"
	"testing"
)

func TestSafeMeiliDocID(t *testing.T) {
	cases := []struct {
		name       string
		entityType string
		entityID   string
		wantPrefix string
		wantExact  string
	}{
		{
			name:       "valid alphanumeric pubkey is preserved",
			entityType: "profile",
			entityID:   "abc123def456",
			wantExact:  "profile_abc123def456",
		},
		{
			name:       "valid id with hyphen and underscore is preserved",
			entityType: "note",
			entityID:   "abc-def_123",
			wantExact:  "note_abc-def_123",
		},
		{
			name:       "nip05 with @ falls back to hash",
			entityType: "identity",
			entityID:   "alice@example.com",
			wantPrefix: "identity_h",
		},
		{
			name:       "domain with . falls back to hash",
			entityType: "identity",
			entityID:   "example.com",
			wantPrefix: "identity_h",
		},
		{
			name:       "unicode hashtag falls back to hash",
			entityType: "hashtag",
			entityID:   "café",
			wantPrefix: "hashtag_h",
		},
		{
			name:       "id over 511 bytes falls back to hash",
			entityType: "note",
			entityID:   strings.Repeat("a", 600),
			wantPrefix: "note_h",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := safeMeiliDocID(tc.entityType, tc.entityID)
			if tc.wantExact != "" && got != tc.wantExact {
				t.Fatalf("got %q, want %q", got, tc.wantExact)
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(got, tc.wantPrefix) {
				t.Fatalf("got %q, want prefix %q", got, tc.wantPrefix)
			}
			if !meiliDocIDValid.MatchString(got) {
				t.Fatalf("safe id %q still contains invalid characters for Meilisearch", got)
			}
			if len(got) > meiliDocIDMaxLen {
				t.Fatalf("safe id length %d exceeds max %d", len(got), meiliDocIDMaxLen)
			}
		})
	}
}

func TestSafeMeiliDocIDIsDeterministic(t *testing.T) {
	id1 := safeMeiliDocID("identity", "alice@example.com")
	id2 := safeMeiliDocID("identity", "alice@example.com")
	if id1 != id2 {
		t.Fatalf("safe id is not deterministic: %q vs %q", id1, id2)
	}
}
