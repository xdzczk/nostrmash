package live

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

func TestDedupCacheLRUEviction(t *testing.T) {
	cache := newDedupCache(3)
	cache.Add("a", "pk-a")
	cache.Add("b", "pk-b")
	cache.Add("c", "pk-c")

	// Touch "a" so "b" becomes the eviction candidate.
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("expected hit for a")
	}
	cache.Add("d", "pk-d")

	if _, ok := cache.Get("b"); ok {
		t.Fatal("expected b to be evicted as least recently used")
	}
	for _, id := range []string{"a", "c", "d"} {
		if _, ok := cache.Get(id); !ok {
			t.Fatalf("expected %s to survive", id)
		}
	}
	if got := cache.Len(); got != 3 {
		t.Fatalf("expected capacity-bounded size 3, got %d", got)
	}
}

func TestDedupCacheDisabled(t *testing.T) {
	if cache := newDedupCache(0); cache != nil {
		t.Fatal("size 0 must disable the cache")
	}
	var nilCache *dedupCache
	nilCache.Add("a", "pk-a")
	if _, ok := nilCache.Get("a"); ok {
		t.Fatal("nil cache must never hit")
	}
}

func TestProcessorDedupShortCircuitsRepeatSightings(t *testing.T) {
	store := &fakeStore{canonicalOutcomes: []bool{true}}
	p, err := NewProcessor(silentLogger(), store, nostr.Options{})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	p.SetDedupCache(16)
	checkpoints := &checkpointRecorder{}
	p.SetCheckpointWriter(checkpoints)

	// First sighting takes the full path and populates the cache.
	if err := p.Handle(context.Background(), "wss://relay.one", []byte(validEventFixture)); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	if store.canonicalWrites != 1 {
		t.Fatalf("expected 1 canonical write, got %d", store.canonicalWrites)
	}

	// Second sighting from another relay must skip the canonical insert and
	// record provenance with the verified pubkey instead.
	if err := p.Handle(context.Background(), "wss://relay.two", []byte(validEventFixture)); err != nil {
		t.Fatalf("second handle: %v", err)
	}
	if store.canonicalWrites != 1 {
		t.Fatalf("cached duplicate must not re-run canonical insert, got %d writes", store.canonicalWrites)
	}
	if len(store.provenanceWrites) != 1 {
		t.Fatalf("expected 1 provenance write, got %d", len(store.provenanceWrites))
	}
	if !strings.Contains(store.provenanceWrites[0], "wss://relay.two") {
		t.Fatalf("provenance must record the sighting relay, got %q", store.provenanceWrites[0])
	}
	if !strings.Contains(store.provenanceWrites[0], "37ce94259421d17a13e04382205c6061323ebc6bbfa46aab1f73e6f93c774a5e") {
		t.Fatalf("provenance must carry the verified pubkey, got %q", store.provenanceWrites[0])
	}
	if checkpoints.calls != 2 {
		t.Fatalf("both sightings must advance checkpoints, got %d", checkpoints.calls)
	}

	counters := p.Snapshot()
	if counters.Valid != 1 || counters.Duplicate != 1 {
		t.Fatalf("unexpected counters: %+v", counters)
	}
}

// BenchmarkHandleDuplicateSighting quantifies the dedup win: how much work a
// repeat sighting of an already-persisted event costs with the cache enabled
// (cheap probe + provenance) versus disabled (full JSON canonicalization,
// SHA-256, Schnorr verify, canonical insert round-trip).
func BenchmarkHandleDuplicateSighting(b *testing.B) {
	for _, enabled := range []bool{true, false} {
		name := "cache_enabled"
		if !enabled {
			name = "cache_disabled"
		}
		b.Run(name, func(b *testing.B) {
			store := &fakeStore{}
			p, err := NewProcessor(silentLogger(), store, nostr.Options{})
			if err != nil {
				b.Fatalf("new processor: %v", err)
			}
			if enabled {
				p.SetDedupCache(1024)
			}
			if err := p.Handle(context.Background(), "wss://relay.one", []byte(validEventFixture)); err != nil {
				b.Fatalf("seed handle: %v", err)
			}
			payload := []byte(validEventFixture)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p.Handle(context.Background(), "wss://relay.two", payload); err != nil {
					b.Fatalf("handle: %v", err)
				}
			}
		})
	}
}

func TestProcessorDedupIgnoresUnknownAndInvalidPayloads(t *testing.T) {
	store := &fakeStore{canonicalOutcomes: []bool{true}}
	p, err := NewProcessor(silentLogger(), store, nostr.Options{})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	p.SetDedupCache(16)

	// A junk payload claiming an uncached ID must fall through to full
	// validation (and be quarantined), never poisoning the cache.
	junk := []byte(`{"id":"deadbeef","created_at":1700000001,"kind":1,"content":"x"}`)
	if err := p.Handle(context.Background(), "wss://relay.one", junk); err != nil {
		t.Fatalf("junk handle: %v", err)
	}
	if len(store.invalidWrites) != 1 {
		t.Fatalf("junk payload must be quarantined, got %d invalid writes", len(store.invalidWrites))
	}
	if _, ok := p.dedup.Get("deadbeef"); ok {
		t.Fatal("invalid payload must not be cached")
	}

	// Junk reusing a cached (verified) ID is skipped safely: the ID is the
	// content hash of the stored event, so the body is irrelevant.
	if err := p.Handle(context.Background(), "wss://relay.one", []byte(validEventFixture)); err != nil {
		t.Fatalf("valid handle: %v", err)
	}
	forged := []byte(fmt.Sprintf(
		`{"id":%q,"created_at":1700000002,"pubkey":"ff","kind":1,"content":"forged"}`,
		"c108c0bfe77ffc3c0e07f1056d0b5d008e2b4e2a8c4197af5b8c7e3582d41f74",
	))
	if err := p.Handle(context.Background(), "wss://relay.one", forged); err != nil {
		t.Fatalf("forged handle: %v", err)
	}
	if len(store.invalidWrites) != 1 {
		t.Fatalf("forged duplicate must not hit validation, got %d invalid writes", len(store.invalidWrites))
	}
	if len(store.provenanceWrites) != 1 || !strings.Contains(store.provenanceWrites[0], "37ce94259421d17a13e04382205c6061323ebc6bbfa46aab1f73e6f93c774a5e") {
		t.Fatalf("forged duplicate provenance must use the verified pubkey, got %v", store.provenanceWrites)
	}
}
