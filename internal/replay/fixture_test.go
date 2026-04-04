package replay

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFixture_DirectoryOrderIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	aPath := filepath.Join(dir, "02-second.ndjson")
	bPath := filepath.Join(dir, "01-first.ndjson")

	if err := os.WriteFile(aPath, []byte(`{"relay_url":"wss://relay.two","payload":{"id":"b"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("write fixture file a: %v", err)
	}
	if err := os.WriteFile(bPath, []byte(`{"relay_url":"wss://relay.one","payload":{"id":"a"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("write fixture file b: %v", err)
	}

	fixture, err := LoadFixture(dir)
	if err != nil {
		t.Fatalf("load fixture dir: %v", err)
	}
	if len(fixture.Entries) != 2 {
		t.Fatalf("unexpected entry count: got=%d want=2", len(fixture.Entries))
	}
	if fixture.Entries[0].RelayURL != "wss://relay.one" {
		t.Fatalf("unexpected first entry order: got=%s", fixture.Entries[0].RelayURL)
	}
	if fixture.Entries[1].RelayURL != "wss://relay.two" {
		t.Fatalf("unexpected second entry order: got=%s", fixture.Entries[1].RelayURL)
	}
}

func TestLoadFixture_RejectsMissingRelayURL(t *testing.T) {
	file := filepath.Join(t.TempDir(), "bad.ndjson")
	if err := os.WriteFile(file, []byte(`{"payload":{"id":"a"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("write bad fixture: %v", err)
	}

	if _, err := LoadFixture(file); err == nil {
		t.Fatal("expected fixture error for missing relay_url")
	}
}
