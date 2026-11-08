package api

import "testing"

func TestParseArchiveDay(t *testing.T) {
	day, ok := parseArchiveDay("2020-01-02")
	if !ok || day != "2020-01-02" {
		t.Fatalf("past day = %q %v", day, ok)
	}
	if _, ok := parseArchiveDay("2020-1-2"); ok {
		t.Fatal("expected a rejected short date")
	}
	if _, ok := parseArchiveDay("not-a-date"); ok {
		t.Fatal("expected a rejected date")
	}
	day, ok = parseArchiveDay("2999-01-01")
	if !ok || day != "" {
		t.Fatalf("future day = %q %v", day, ok)
	}
}
