package store

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestIngestCheckpointUpsertAndLoad(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ps := NewPostgresStore(pool)
	since := int64(100)
	until := int64(200)
	cursor := "140"
	lastEventID := "evt-140"
	eoseAt := time.Date(2026, 4, 4, 16, 0, 0, 0, time.UTC)
	progressAt := time.Date(2026, 4, 4, 16, 1, 0, 0, time.UTC)
	lastError := "temporary relay timeout"
	initial := model.IngestCheckpoint{
		RelayURL:       "wss://relay.one",
		Mode:           model.ModeBackfill,
		FilterGroup:    "default_v1",
		Since:          &since,
		Until:          &until,
		Cursor:         &cursor,
		LastEventID:    &lastEventID,
		LastProgressAt: &progressAt,
		EOSESeenAt:     &eoseAt,
		Status:         model.CheckpointRunning,
		LastError:      &lastError,
	}
	if err := ps.UpsertIngestCheckpoint(ctx, initial); err != nil {
		t.Fatalf("upsert initial checkpoint: %v", err)
	}

	loaded, err := ps.GetIngestCheckpoint(ctx, "wss://relay.one", model.ModeBackfill, "default_v1")
	if err != nil {
		t.Fatalf("load initial checkpoint: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint row")
	}
	if loaded.Cursor == nil || *loaded.Cursor != "140" {
		t.Fatalf("cursor mismatch: got %v want 140", loaded.Cursor)
	}
	if loaded.Status != model.CheckpointRunning {
		t.Fatalf("status mismatch: got %q want %q", loaded.Status, model.CheckpointRunning)
	}
	if loaded.EOSESeenAt == nil || !loaded.EOSESeenAt.Equal(eoseAt) {
		t.Fatalf("eose_seen_at mismatch: got %v want %s", loaded.EOSESeenAt, eoseAt)
	}
	if loaded.LastEventID == nil || *loaded.LastEventID != lastEventID {
		t.Fatalf("last_event_id mismatch: got %v want %s", loaded.LastEventID, lastEventID)
	}
	if loaded.LastProgressAt == nil || !loaded.LastProgressAt.Equal(progressAt) {
		t.Fatalf("last_progress_at mismatch: got %v want %s", loaded.LastProgressAt, progressAt)
	}
	if loaded.LastError == nil || *loaded.LastError != lastError {
		t.Fatalf("last_error mismatch: got %v want %s", loaded.LastError, lastError)
	}

	cursor2 := "190"
	updated := model.IngestCheckpoint{
		RelayURL:    "wss://relay.one",
		Mode:        model.ModeBackfill,
		FilterGroup: "default_v1",
		Since:       &since,
		Until:       &until,
		Cursor:      &cursor2,
		Status:      model.CheckpointCompleted,
		LastError:   nil,
	}
	if err := ps.UpsertIngestCheckpoint(ctx, updated); err != nil {
		t.Fatalf("upsert updated checkpoint: %v", err)
	}
	loaded, err = ps.GetIngestCheckpoint(ctx, "wss://relay.one", model.ModeBackfill, "default_v1")
	if err != nil {
		t.Fatalf("load updated checkpoint: %v", err)
	}
	if loaded.Status != model.CheckpointCompleted {
		t.Fatalf("status mismatch: got %q want %q", loaded.Status, model.CheckpointCompleted)
	}
	if loaded.Cursor == nil || *loaded.Cursor != "190" {
		t.Fatalf("cursor mismatch: got %v want 190", loaded.Cursor)
	}
	if loaded.LastError != nil {
		t.Fatalf("last_error should be cleared: got %v", *loaded.LastError)
	}
}
