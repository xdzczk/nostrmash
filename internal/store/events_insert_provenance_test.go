package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestInsertCanonicalEventAccumulatesProvenanceAcrossRelays(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	store := NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 11, 0, 0, 0, time.UTC)
	eventID := "event_2"
	event := model.Event{
		ID:          eventID,
		Pubkey:      "pubkey_x",
		CreatedAt:   22222,
		Kind:        1,
		Sig:         "sig_x",
		Content:     "hi",
		RawJSON:     json.RawMessage(`{"id":"event_2","kind":1}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	tags := [][]string{{"e", "root"}}

	if err := store.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("insert relay one: %v", err)
	}
	if err := store.InsertCanonicalEvent(ctx, event, tags, "wss://relay.two", baseTime.Add(3*time.Minute)); err != nil {
		t.Fatalf("insert relay two: %v", err)
	}
	// Duplicate provenance key should be idempotent and preserve earliest seen_at.
	if err := store.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", baseTime.Add(-1*time.Minute)); err != nil {
		t.Fatalf("reinsert relay one earlier: %v", err)
	}

	var relayCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM event_relays WHERE event_id = $1`, eventID).Scan(&relayCount); err != nil {
		t.Fatalf("count event_relays: %v", err)
	}
	if relayCount != 2 {
		t.Fatalf("expected 2 relay rows, got %d", relayCount)
	}

	var relayOneSeenAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT seen_at FROM event_relays WHERE event_id = $1 AND relay_url = $2
	`, eventID, "wss://relay.one").Scan(&relayOneSeenAt); err != nil {
		t.Fatalf("query relay one seen_at: %v", err)
	}
	expectedRelayOneSeenAt := baseTime.Add(-1 * time.Minute)
	if !relayOneSeenAt.Equal(expectedRelayOneSeenAt) {
		t.Fatalf("relay one seen_at mismatch: got %s want %s", relayOneSeenAt, expectedRelayOneSeenAt)
	}
}
