package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestPurgeInvalidEventsOlderThan_DeletesOldRowsOnly(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	store := NewPostgresStore(pool)
	if err := store.InsertInvalidEvent(ctx, model.InvalidEvent{
		ErrorCode:    "bad_sig",
		ErrorMessage: "sig invalid",
		RawPayload:   json.RawMessage(`{"id":"old-1"}`),
		SeenAt:       time.Now().UTC().Add(-48 * time.Hour),
	}); err != nil {
		t.Fatalf("insert old invalid event 1: %v", err)
	}
	if err := store.InsertInvalidEvent(ctx, model.InvalidEvent{
		ErrorCode:    "bad_sig",
		ErrorMessage: "sig invalid",
		RawPayload:   json.RawMessage(`{"id":"old-2"}`),
		SeenAt:       time.Now().UTC().Add(-72 * time.Hour),
	}); err != nil {
		t.Fatalf("insert old invalid event 2: %v", err)
	}
	if err := store.InsertInvalidEvent(ctx, model.InvalidEvent{
		ErrorCode:    "bad_sig",
		ErrorMessage: "sig invalid",
		RawPayload:   json.RawMessage(`{"id":"new-1"}`),
		SeenAt:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("insert new invalid event: %v", err)
	}

	deleted, err := store.PurgeInvalidEventsOlderThan(ctx, time.Now().UTC().Add(-24*time.Hour), 100)
	if err != nil {
		t.Fatalf("purge invalid events: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted rows, got %d", deleted)
	}

	var remaining int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM invalid_events`).Scan(&remaining); err != nil {
		t.Fatalf("count invalid_events: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected 1 remaining invalid_event row, got %d", remaining)
	}
}

func TestTrimInvalidEventPayloadsOlderThan_NullsOnlyOldPayloads(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	store := NewPostgresStore(pool)
	if err := store.InsertInvalidEvent(ctx, model.InvalidEvent{
		ErrorCode:    "old_trim",
		ErrorMessage: "old payload",
		RawPayload:   json.RawMessage(`{"id":"old-trim"}`),
		SeenAt:       time.Now().UTC().Add(-72 * time.Hour),
	}); err != nil {
		t.Fatalf("insert old invalid event: %v", err)
	}
	if err := store.InsertInvalidEvent(ctx, model.InvalidEvent{
		ErrorCode:    "new_keep",
		ErrorMessage: "new payload",
		RawPayload:   json.RawMessage(`{"id":"new-keep"}`),
		SeenAt:       time.Now().UTC().Add(-2 * time.Hour),
	}); err != nil {
		t.Fatalf("insert new invalid event: %v", err)
	}

	trimmed, err := store.TrimInvalidEventPayloadsOlderThan(ctx, time.Now().UTC().Add(-24*time.Hour), 100)
	if err != nil {
		t.Fatalf("trim invalid event payloads: %v", err)
	}
	if trimmed != 1 {
		t.Fatalf("expected 1 trimmed payload row, got %d", trimmed)
	}

	var oldPayloadBytes []byte
	if err := pool.QueryRow(ctx, `SELECT raw_payload::text FROM invalid_events WHERE error_code = 'old_trim'`).Scan(&oldPayloadBytes); err != nil {
		t.Fatalf("query old payload: %v", err)
	}
	if len(oldPayloadBytes) != 0 {
		t.Fatalf("expected old payload to be nulled, got %s", string(oldPayloadBytes))
	}

	var newPayloadBytes []byte
	if err := pool.QueryRow(ctx, `SELECT raw_payload::text FROM invalid_events WHERE error_code = 'new_keep'`).Scan(&newPayloadBytes); err != nil {
		t.Fatalf("query new payload: %v", err)
	}
	if len(newPayloadBytes) == 0 {
		t.Fatalf("expected new payload to remain present")
	}
}
