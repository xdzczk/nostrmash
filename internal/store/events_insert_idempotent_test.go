package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestInsertCanonicalEventIdempotentOnIDAndPreservesEarliestFirstSeen(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	store := NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 10, 0, 0, 0, time.UTC)

	eventID := "event_1"
	firstRaw := json.RawMessage(`{"id":"event_1","kind":1,"content":"hello"}`)
	firstEvent := model.Event{
		ID:          eventID,
		Pubkey:      "pubkey_a",
		CreatedAt:   12345,
		Kind:        1,
		Sig:         "sig_a",
		Content:     "hello",
		RawJSON:     firstRaw,
		FirstSeenAt: baseTime.Add(10 * time.Minute),
		InsertedAt:  baseTime.Add(10 * time.Minute),
	}

	tags := [][]string{
		{"e", "root", "wss://relay.hint"},
		{"p", "author"},
		{"client", "nostrmash"},
	}

	if err := store.InsertCanonicalEvent(ctx, firstEvent, tags, "wss://relay.one", baseTime.Add(10*time.Minute)); err != nil {
		t.Fatalf("first insert canonical event: %v", err)
	}

	// Same event ID should be idempotent for canonical payload fields,
	// but earliest first_seen should still move backward.
	secondEvent := model.Event{
		ID:          eventID,
		Pubkey:      "pubkey_b_should_not_overwrite",
		CreatedAt:   99999,
		Kind:        2,
		Sig:         "sig_b_should_not_overwrite",
		Content:     "changed content should not overwrite",
		RawJSON:     json.RawMessage(`{"id":"event_1","content":"changed"}`),
		FirstSeenAt: baseTime.Add(-5 * time.Minute),
		InsertedAt:  baseTime.Add(15 * time.Minute),
	}
	if err := store.InsertCanonicalEvent(ctx, secondEvent, tags, "wss://relay.one", baseTime.Add(-5*time.Minute)); err != nil {
		t.Fatalf("second insert canonical event: %v", err)
	}

	var (
		pubkey      string
		createdAt   int64
		kind        int
		sig         string
		content     string
		rawJSON     []byte
		firstSeenAt time.Time
	)
	err := pool.QueryRow(ctx, `
		SELECT pubkey, created_at, kind, sig, content, raw_json::text, first_seen_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &createdAt, &kind, &sig, &content, &rawJSON, &firstSeenAt)
	if err != nil {
		t.Fatalf("query canonical event: %v", err)
	}

	if pubkey != firstEvent.Pubkey || createdAt != firstEvent.CreatedAt || kind != firstEvent.Kind || sig != firstEvent.Sig || content != firstEvent.Content {
		t.Fatalf("canonical fields were overwritten on duplicate event id")
	}
	if !jsonEqual(rawJSON, firstRaw) {
		t.Fatalf("raw_json was overwritten on duplicate event id; got %s want %s", string(rawJSON), string(firstRaw))
	}
	expectedFirstSeen := baseTime.Add(-5 * time.Minute)
	if !firstSeenAt.Equal(expectedFirstSeen) {
		t.Fatalf("first_seen_at mismatch: got %s want %s", firstSeenAt, expectedFirstSeen)
	}

	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE id = $1`, eventID).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("expected 1 events row, got %d", eventCount)
	}

	var tagCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM event_tags WHERE event_id = $1`, eventID).Scan(&tagCount); err != nil {
		t.Fatalf("count event_tags: %v", err)
	}
	if tagCount != 4 {
		t.Fatalf("expected 4 tag rows, got %d", tagCount)
	}

	var relayCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM event_relays WHERE event_id = $1`, eventID).Scan(&relayCount); err != nil {
		t.Fatalf("count event_relays: %v", err)
	}
	if relayCount != 1 {
		t.Fatalf("expected 1 relay row, got %d", relayCount)
	}
}
