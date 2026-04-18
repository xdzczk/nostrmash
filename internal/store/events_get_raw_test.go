package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetEventRawByIDAndBatchQueries(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 13, 0, 0, 0, time.UTC)
	eventA := model.Event{
		ID:          "event_query_a",
		Pubkey:      "pub_a",
		CreatedAt:   111,
		Kind:        1,
		Sig:         "sig_a",
		Content:     "a",
		RawJSON:     json.RawMessage(`{"id":"event_query_a","kind":1}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	eventB := model.Event{
		ID:          "event_query_b",
		Pubkey:      "pub_b",
		CreatedAt:   222,
		Kind:        1,
		Sig:         "sig_b",
		Content:     "b",
		RawJSON:     json.RawMessage(`{"id":"event_query_b","kind":1}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	if err := s.InsertCanonicalEvent(ctx, eventA, nil, "wss://relay.one", baseTime); err != nil {
		t.Fatalf("insert event a: %v", err)
	}
	if err := s.InsertCanonicalEvent(ctx, eventB, nil, "wss://relay.two", baseTime); err != nil {
		t.Fatalf("insert event b: %v", err)
	}

	raw, err := s.GetEventRawByID(ctx, "event_query_a")
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	if !jsonEqual(raw, eventA.RawJSON) {
		t.Fatalf("event raw mismatch: got %s want %s", string(raw), string(eventA.RawJSON))
	}

	batch, err := s.GetEventRawsByIDs(ctx, []string{"event_query_a", "event_query_b", "missing"})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 found events, got %d", len(batch))
	}
	if !jsonEqual(batch["event_query_b"], eventB.RawJSON) {
		t.Fatalf("event b raw mismatch: got %s want %s", string(batch["event_query_b"]), string(eventB.RawJSON))
	}
}
