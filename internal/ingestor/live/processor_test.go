package live

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestProcessorHandle_ValidDuplicateInvalidCounters(t *testing.T) {
	t.Parallel()

	fake := &fakeStore{
		canonicalOutcomes: []bool{true, false},
	}
	processor, err := NewProcessor(silentLogger(), fake, nostr.Options{})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}

	if err := processor.Handle(context.Background(), "wss://relay.one", []byte(validEventFixture)); err != nil {
		t.Fatalf("valid event handle: %v", err)
	}
	if err := processor.Handle(context.Background(), "wss://relay.one", []byte(validEventFixture)); err != nil {
		t.Fatalf("duplicate event handle: %v", err)
	}
	if err := processor.Handle(context.Background(), "wss://relay.one", []byte(`{"not":"nostr"}`)); err != nil {
		t.Fatalf("invalid event handle: %v", err)
	}

	counters := processor.Snapshot()
	if counters.Valid != 1 || counters.Duplicate != 1 || counters.Invalid != 1 {
		t.Fatalf("unexpected counters: %+v", counters)
	}
	if len(fake.invalidWrites) != 1 {
		t.Fatalf("expected one invalid write, got %d", len(fake.invalidWrites))
	}
	if fake.invalidWrites[0].ErrorCode == "" {
		t.Fatal("invalid write should include an error code")
	}
}

const validEventFixture = `{
  "kind": 1,
  "created_at": 1700000000,
  "tags": [
    ["client", "nostrmash"],
    ["t", "test"]
  ],
  "content": "hello from nostrmash fixture",
  "pubkey": "37ce94259421d17a13e04382205c6061323ebc6bbfa46aab1f73e6f93c774a5e",
  "id": "c108c0bfe77ffc3c0e07f1056d0b5d008e2b4e2a8c4197af5b8c7e3582d41f74",
  "sig": "18cefd294b6f23128c635d1e49742b0191b4a6f61179af750d0b06a6f6bde6086a6743380fd368d146236cdd2a3e795180a0176baf96c50e3d789cdec72d26eb"
}`

type fakeStore struct {
	canonicalOutcomes []bool
	canonicalWrites   int
	invalidWrites     []model.InvalidEvent
}

func (f *fakeStore) InsertCanonicalEventWithResult(
	ctx context.Context,
	event model.Event,
	tags [][]string,
	relayURL string,
	relaySeenAt time.Time,
) (store.CanonicalInsertResult, error) {
	inserted := true
	if f.canonicalWrites < len(f.canonicalOutcomes) {
		inserted = f.canonicalOutcomes[f.canonicalWrites]
	}
	f.canonicalWrites++
	return store.CanonicalInsertResult{EventInserted: inserted}, nil
}

func (f *fakeStore) InsertInvalidEvent(ctx context.Context, invalid model.InvalidEvent) error {
	f.invalidWrites = append(f.invalidWrites, invalid)
	return nil
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
