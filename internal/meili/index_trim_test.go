package meili

import (
	"strings"
	"testing"
	"time"
)

func TestTruncateIndexedText(t *testing.T) {
	t.Parallel()
	if got := truncateIndexedText("  hello  ", 10); got != "hello" {
		t.Fatalf("short text: got %q", got)
	}
	long := strings.Repeat("ä", 500)
	got := truncateIndexedText(long, 480)
	if strings.Count(got, "ä") != 480 {
		t.Fatalf("expected 480 runes, got %d", strings.Count(got, "ä"))
	}
}

func TestStripOpaqueTokens(t *testing.T) {
	t.Parallel()
	hexID := strings.Repeat("ab", 32)
	cases := []struct {
		name, in, want string
	}{
		{"plain", "hello  world", "hello world"},
		{"nostr ref", "see nostr:nevent1qqsw4ckm0h8yq6x2p8v6k3tj5g9fyk4slt6w5vq9m2ulhdj7mpa7n0zq4w3xf9 for details", "see for details"},
		{"bare npub", "by npub1sn0wdenkukak0d9dfczzeacvhkrgz92ak56egt7vdgzn8pv2wfqqhrjdv9 today", "by today"},
		{"hex id", "ref " + hexID + " done", "ref done"},
		{"url to host", "watch https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=1s now", "watch www.youtube.com now"},
		{"relay url", "wss://relay.damus.io/ is up", "relay.damus.io is up"},
		{"invoice", "pay lnbc10u1p3xnhl2pp5j5t2d3ub5a0wjvqnk8y2v4wvl2d7q7cxsp5l2fv93kyldm7fsdq5ssrzjqw5r4vgyvd3hwv9s", "pay"},
		{"base64 blob", "img " + strings.Repeat("QUJD", 20) + " end", "img end"},
		{"keeps short hex-looking words", "cafe babe deadbeef", "cafe babe deadbeef"},
		{"keeps lightning address", "zap me at alice@getalby.com", "zap me at alice@getalby.com"},
	}
	for _, tc := range cases {
		if got := stripOpaqueTokens(tc.in); got != tc.want {
			t.Fatalf("%s: stripOpaqueTokens(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}

func TestTrimNoteDocumentStripsBeforeTruncating(t *testing.T) {
	t.Parallel()
	row := NoteDocument{Content: strings.Repeat("word ", 88) + "https://example.com/" + strings.Repeat("p", 200) + " tail"}
	trimNoteDocument(&row)
	if !strings.HasSuffix(row.Content, " example.com tail") {
		t.Fatalf("expected URL collapsed to host before truncation, got %q", row.Content)
	}
}

func TestTrimProfileDocumentDropsProfileJSON(t *testing.T) {
	t.Parallel()
	row := ProfileDocument{
		About:       strings.Repeat("word ", 80),
		ProfileJSON: []byte(`{"name":"n","lud16":"a@b.c"}`),
	}
	trimProfileDocument(&row)
	if row.ProfileJSON != nil {
		t.Fatalf("expected profile_json cleared, got %s", row.ProfileJSON)
	}
	if len([]rune(row.About)) != indexedAboutMaxRunes {
		t.Fatalf("about length=%d want %d", len([]rune(row.About)), indexedAboutMaxRunes)
	}
}

func TestIndexedNotesMinCreatedAt(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	min := indexedNotesMinCreatedAt(now)
	want := now.Add(-indexedNotesMaxAge).Unix()
	if min != want {
		t.Fatalf("min=%d want=%d", min, want)
	}
}

func TestFilterMeiliDocumentsIndexRows(t *testing.T) {
	t.Parallel()
	in := []SearchDocument{
		{EntityType: "note", EntityID: "n1"},
		{EntityType: "profile", EntityID: "p1"},
		{EntityType: "hashtag", EntityID: "bitcoin"},
		{EntityType: "identity", EntityID: "alice"},
		{EntityType: "relay", EntityID: "wss://x"},
	}
	got := filterMeiliDocumentsIndexRows(in)
	if len(got) != 3 {
		t.Fatalf("len=%d want 3: %#v", len(got), got)
	}
	for _, row := range got {
		switch row.EntityType {
		case "hashtag", "identity", "relay":
		default:
			t.Fatalf("unexpected entity_type %q", row.EntityType)
		}
	}
}
