package live

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/ingestor/relay"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestCheckpointTracker_PersistsLifecycleTimestamps(t *testing.T) {
	store := &trackerStoreStub{}
	tracker, err := NewCheckpointTracker(slog.Default(), store, "default_v1", time.Hour)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	base := time.Date(2026, 4, 5, 11, 0, 0, 0, time.UTC)
	tracker.nowFn = func() time.Time { return base }

	if err := tracker.SetRelayStatus(context.Background(), "wss://relay.one", relay.StateHealthy, ""); err != nil {
		t.Fatalf("set healthy: %v", err)
	}
	base = base.Add(2 * time.Minute)
	if err := tracker.SetRelayStatus(context.Background(), "wss://relay.one", relay.StateErrored, "timeout"); err != nil {
		t.Fatalf("set errored: %v", err)
	}

	latest := store.latest("wss://relay.one")
	if latest == nil {
		t.Fatal("expected persisted checkpoint")
	}
	if latest.LastConnectedAt == nil {
		t.Fatal("expected last_connected_at")
	}
	if latest.LastDisconnectedAt == nil {
		t.Fatal("expected last_disconnected_at")
	}
	if latest.LastError == nil || *latest.LastError != "timeout" {
		t.Fatalf("unexpected last_error: %v", latest.LastError)
	}
	if latest.LastErrorAt == nil {
		t.Fatal("expected last_error_at")
	}
	if latest.Status != model.CheckpointErrored {
		t.Fatalf("unexpected status: %s", latest.Status)
	}
}

func TestCheckpointTracker_ReconnectCountAndStateNormalization(t *testing.T) {
	store := &trackerStoreStub{}
	tracker, err := NewCheckpointTracker(slog.Default(), store, "default_v1", time.Hour)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}

	if err := tracker.SetRelayStatus(context.Background(), "wss://relay.one", relay.StateConnecting, ""); err != nil {
		t.Fatalf("set connecting #1: %v", err)
	}
	if err := tracker.SetRelayStatus(context.Background(), "wss://relay.one", relay.StateConnecting, ""); err != nil {
		t.Fatalf("set connecting #2: %v", err)
	}
	if err := tracker.SetRelayStatus(context.Background(), "wss://relay.one", relay.StateLagging, ""); err != nil {
		t.Fatalf("set lagging: %v", err)
	}
	if err := tracker.SetRelayStatus(context.Background(), "wss://relay.one", relay.StateDisconnected, ""); err != nil {
		t.Fatalf("set disconnected: %v", err)
	}

	latest := store.latest("wss://relay.one")
	if latest == nil {
		t.Fatal("expected persisted checkpoint")
	}
	if latest.ReconnectCount != 2 {
		t.Fatalf("unexpected reconnect count: got=%d want=2", latest.ReconnectCount)
	}
	if latest.Status != model.CheckpointDisconnected {
		t.Fatalf("unexpected final status: %s", latest.Status)
	}
	if latest.LastDisconnectedAt == nil {
		t.Fatal("expected last_disconnected_at")
	}
}

type trackerStoreStub struct {
	checkpoints map[string]model.IngestCheckpoint
}

func (s *trackerStoreStub) GetIngestCheckpoint(
	_ context.Context,
	relayURL string,
	mode string,
	filterGroup string,
) (*model.IngestCheckpoint, error) {
	if s.checkpoints == nil {
		return nil, nil
	}
	cp, ok := s.checkpoints[relayURL+"|"+mode+"|"+filterGroup]
	if !ok {
		return nil, nil
	}
	c := cp
	return &c, nil
}

func (s *trackerStoreStub) UpsertIngestCheckpoint(_ context.Context, checkpoint model.IngestCheckpoint) error {
	if s.checkpoints == nil {
		s.checkpoints = make(map[string]model.IngestCheckpoint)
	}
	key := checkpoint.RelayURL + "|" + checkpoint.Mode + "|" + checkpoint.FilterGroup
	s.checkpoints[key] = checkpoint
	return nil
}

func (s *trackerStoreStub) latest(relayURL string) *model.IngestCheckpoint {
	if s.checkpoints == nil {
		return nil
	}
	key := relayURL + "|" + model.ModeLive + "|default_v1"
	cp, ok := s.checkpoints[key]
	if !ok {
		return nil
	}
	c := cp
	return &c
}
