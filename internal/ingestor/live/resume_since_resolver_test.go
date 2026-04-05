package live

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestResumeSinceResolver_NoCheckpointUsesBootstrapLookback(t *testing.T) {
	store := &fakeCheckpointStore{}
	resolver, err := NewResumeSinceResolver(store, "default_v1", 300, 60)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	resolver.nowFn = func() time.Time {
		return time.Unix(2_000, 0).UTC()
	}

	got, err := resolver.ResolveSince(context.Background(), "wss://relay.one")
	if err != nil {
		t.Fatalf("resolve since: %v", err)
	}
	if got.Strategy != "bootstrap_lookback" {
		t.Fatalf("strategy mismatch: got %q", got.Strategy)
	}
	if got.Since != 1700 {
		t.Fatalf("since mismatch: got %d want 1700", got.Since)
	}
}

func TestResumeSinceResolver_CheckpointUsesOverlap(t *testing.T) {
	checkpointSince := int64(1_000)
	store := &fakeCheckpointStore{
		checkpoint: &model.IngestCheckpoint{
			RelayURL:    "wss://relay.one",
			Mode:        model.ModeLive,
			FilterGroup: "default_v1",
			Since:       &checkpointSince,
			Status:      model.CheckpointHealthy,
		},
	}
	resolver, err := NewResumeSinceResolver(store, "default_v1", 300, 60)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	got, err := resolver.ResolveSince(context.Background(), "wss://relay.one")
	if err != nil {
		t.Fatalf("resolve since: %v", err)
	}
	if got.Strategy != "checkpoint" {
		t.Fatalf("strategy mismatch: got %q", got.Strategy)
	}
	if got.Since != 940 {
		t.Fatalf("since mismatch: got %d want 940", got.Since)
	}
}

func TestResumeSinceResolver_ClampNegativeSinceToZero(t *testing.T) {
	checkpointSince := int64(30)
	store := &fakeCheckpointStore{
		checkpoint: &model.IngestCheckpoint{
			RelayURL:    "wss://relay.one",
			Mode:        model.ModeLive,
			FilterGroup: "default_v1",
			Since:       &checkpointSince,
			Status:      model.CheckpointHealthy,
		},
	}
	resolver, err := NewResumeSinceResolver(store, "default_v1", 300, 120)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	got, err := resolver.ResolveSince(context.Background(), "wss://relay.one")
	if err != nil {
		t.Fatalf("resolve since: %v", err)
	}
	if got.Since != 0 {
		t.Fatalf("since mismatch: got %d want 0", got.Since)
	}
}

type fakeCheckpointStore struct {
	checkpoint *model.IngestCheckpoint
	upserts    []model.IngestCheckpoint
}

func (f *fakeCheckpointStore) GetIngestCheckpoint(
	ctx context.Context,
	relayURL string,
	mode string,
	filterGroup string,
) (*model.IngestCheckpoint, error) {
	if f.checkpoint == nil {
		return nil, nil
	}
	cp := *f.checkpoint
	return &cp, nil
}

func (f *fakeCheckpointStore) UpsertIngestCheckpoint(
	ctx context.Context,
	checkpoint model.IngestCheckpoint,
) error {
	f.upserts = append(f.upserts, checkpoint)
	return nil
}
