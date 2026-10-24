package query

import "testing"

func TestNormalizeHashtagForLookupRejectsNumeric(t *testing.T) {
	if got := normalizeHashtagForLookup("#Nostr"); got != "nostr" {
		t.Fatalf("got %q", got)
	}
	if got := normalizeHashtagForLookup("#1"); got != "" {
		t.Fatalf("numeric hashtag should be empty, got %q", got)
	}
	if got := normalizeHashtagForLookup("42"); got != "" {
		t.Fatalf("numeric hashtag should be empty, got %q", got)
	}
}

func TestHashtagsFromContentSkipsNumeric(t *testing.T) {
	got := hashtagsFromContent("the #1 app in the store, also #nostr and #42")
	if len(got) != 1 || got[0] != "nostr" {
		t.Fatalf("got %#v", got)
	}
}
