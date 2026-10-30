package meili

import (
	"strings"
	"testing"

	ms "github.com/meilisearch/meilisearch-go"
)

func TestHexPubkeyQuery(t *testing.T) {
	hexKey := strings.Repeat("ab", 32)
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{hexKey, hexKey, true},
		{"  " + strings.ToUpper(hexKey) + " ", hexKey, true},
		{hexKey[:63], "", false},
		{hexKey + "0", "", false},
		{strings.Repeat("zz", 32), "", false},
		{"nostr", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		got, ok := hexPubkeyQuery(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("hexPubkeyQuery(%q) = (%q, %v), want (%q, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestApplyHexPubkeyFilter(t *testing.T) {
	hexKey := strings.Repeat("cd", 32)

	req := &ms.SearchRequest{}
	if q := applyHexPubkeyFilter(req, hexKey); q != "" {
		t.Fatalf("expected identifier query to be cleared, got %q", q)
	}
	if req.Filter != `pubkey = "`+hexKey+`"` {
		t.Fatalf("unexpected filter %#v", req.Filter)
	}

	req = &ms.SearchRequest{}
	if q := applyHexPubkeyFilter(req, "alice"); q != "alice" {
		t.Fatalf("expected free-text query to pass through, got %q", q)
	}
	if req.Filter != nil {
		t.Fatalf("expected no filter for free-text query, got %#v", req.Filter)
	}
}

// Hash-shaped identifiers must never be searchable: Meilisearch rebuilds its
// word/prefix databases per task, and a stream of unique 64-hex tokens made a
// one-document update to `notes` take ~27s in production.
func TestIndexSpecsKeepIdentifiersOutOfSearchableAttributes(t *testing.T) {
	banned := map[string]map[string]bool{
		IndexNotes:    {"id": true, "pubkey": true},
		IndexProfiles: {"pubkey": true},
	}
	for _, spec := range meiliIndexSpecs() {
		if spec.Settings.ProximityPrecision != ms.ByAttribute {
			t.Fatalf("index %s proximityPrecision = %q, want byAttribute", spec.UID, spec.Settings.ProximityPrecision)
		}
		for _, attr := range spec.Settings.SearchableAttributes {
			if banned[spec.UID][attr] {
				t.Fatalf("index %s must not have %q in searchableAttributes", spec.UID, attr)
			}
			if spec.UID == IndexDocuments && attr == "entity_id" {
				t.Fatalf("documents index must not search entity_id")
			}
		}
		if spec.UID != IndexNotes {
			continue
		}
		for _, attr := range spec.Settings.FilterableAttributes {
			if attr == "id" {
				t.Fatalf("notes index must not filter on id")
			}
		}
		displayed := map[string]bool{}
		for _, attr := range spec.Settings.DisplayedAttributes {
			displayed[attr] = true
		}
		if !displayed["event_id"] {
			t.Fatalf("notes index must display event_id so live activities hydrate the Nostr event")
		}
	}
}
