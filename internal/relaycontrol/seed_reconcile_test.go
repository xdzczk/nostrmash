package relaycontrol

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

type fakeSeedRelayStore struct {
	upsertCalls [][2]string
	clearCalls  [][]string
	clearRes    int64
	upsertErr   error
	clearErr    error
}

func (f *fakeSeedRelayStore) UpsertSeedRelay(_ context.Context, urlKey, normalizedURL string) error {
	f.upsertCalls = append(f.upsertCalls, [2]string{urlKey, normalizedURL})
	return f.upsertErr
}

func (f *fakeSeedRelayStore) ClearMissingSeedRelays(_ context.Context, keepURLKeys []string) (int64, error) {
	f.clearCalls = append(f.clearCalls, append([]string(nil), keepURLKeys...))
	return f.clearRes, f.clearErr
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestReconcileSeedRelays_UpsertsConfiguredAndClearsMissing(t *testing.T) {
	store := &fakeSeedRelayStore{clearRes: 1}
	cfg := config.RelayRegistryConfig{
		SeedRelays: []string{
			"wss://relay.primal.net",
			"wss://nos.lol",
		},
	}

	if err := reconcileSeedRelays(context.Background(), testLogger(), store, cfg); err != nil {
		t.Fatalf("reconcileSeedRelays: %v", err)
	}
	if len(store.upsertCalls) != 2 {
		t.Fatalf("expected 2 upserts, got %d", len(store.upsertCalls))
	}
	if len(store.clearCalls) != 1 {
		t.Fatalf("expected 1 clear call, got %d", len(store.clearCalls))
	}

	wantKeys := []string{
		relayurl.CanonicalKey("wss://relay.primal.net"),
		relayurl.CanonicalKey("wss://nos.lol"),
	}
	gotKeys := store.clearCalls[0]
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("clear keep keys = %v, want %v", gotKeys, wantKeys)
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("clear keep keys[%d] = %q, want %q", i, gotKeys[i], wantKeys[i])
		}
	}
}

func TestReconcileSeedRelays_EmptySeedsStillClearsFormerSeeds(t *testing.T) {
	store := &fakeSeedRelayStore{clearRes: 3}
	cfg := config.RelayRegistryConfig{SeedRelays: nil}

	if err := reconcileSeedRelays(context.Background(), testLogger(), store, cfg); err != nil {
		t.Fatalf("reconcileSeedRelays: %v", err)
	}
	if len(store.upsertCalls) != 0 {
		t.Fatalf("expected no upserts, got %d", len(store.upsertCalls))
	}
	if len(store.clearCalls) != 1 {
		t.Fatalf("expected 1 clear call, got %d", len(store.clearCalls))
	}
	if len(store.clearCalls[0]) != 0 {
		t.Fatalf("empty seed list must clear with empty keep set, got %v", store.clearCalls[0])
	}
}

func TestReconcileSeedRelays_SkipsInvalidURLsInKeepSet(t *testing.T) {
	store := &fakeSeedRelayStore{}
	cfg := config.RelayRegistryConfig{
		SeedRelays: []string{
			"wss://relay.primal.net",
			"not-a-relay-url",
		},
	}

	if err := reconcileSeedRelays(context.Background(), testLogger(), store, cfg); err != nil {
		t.Fatalf("reconcileSeedRelays: %v", err)
	}
	if len(store.upsertCalls) != 1 {
		t.Fatalf("expected 1 successful upsert, got %d", len(store.upsertCalls))
	}
	if got, want := len(store.clearCalls[0]), 1; got != want {
		t.Fatalf("keep set size = %d, want %d (%v)", got, want, store.clearCalls[0])
	}
}

func TestReconcileSeedRelays_PropagatesClearError(t *testing.T) {
	store := &fakeSeedRelayStore{clearErr: errors.New("boom")}
	cfg := config.RelayRegistryConfig{SeedRelays: []string{"wss://nos.lol"}}

	if err := reconcileSeedRelays(context.Background(), testLogger(), store, cfg); err == nil {
		t.Fatal("expected clear error")
	}
}

func TestReconcileSeedRelays_KeepsConfiguredSeedsOnUpsertError(t *testing.T) {
	store := &fakeSeedRelayStore{upsertErr: errors.New("upsert failed"), clearRes: 0}
	cfg := config.RelayRegistryConfig{
		SeedRelays: []string{"wss://relay.primal.net", "wss://nos.lol"},
	}

	if err := reconcileSeedRelays(context.Background(), testLogger(), store, cfg); err != nil {
		t.Fatalf("reconcileSeedRelays: %v", err)
	}
	if len(store.upsertCalls) != 2 {
		t.Fatalf("expected 2 upsert attempts, got %d", len(store.upsertCalls))
	}
	// Configured seeds must remain in the keep set so a write failure cannot
	// accidentally unpin them on the subsequent clear.
	if got, want := len(store.clearCalls[0]), 2; got != want {
		t.Fatalf("keep set size = %d, want %d (%v)", got, want, store.clearCalls[0])
	}
}
