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

func TestTrimProfileDocumentDropsProfileJSON(t *testing.T) {
	t.Parallel()
	row := ProfileDocument{
		About:       strings.Repeat("x", 400),
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
