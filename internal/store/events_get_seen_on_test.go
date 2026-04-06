package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetEventSeenOn(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 14, 0, 0, 0, time.UTC)
	event := model.Event{
		ID:          "event_seen_on",
		Pubkey:      "pub_seen_on",
		CreatedAt:   333,
		Kind:        1,
		Sig:         "sig_seen_on",
		Content:     "seen-on",
		RawJSON:     json.RawMessage(`{"id":"event_seen_on","kind":1}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	if err := s.InsertCanonicalEvent(ctx, event, nil, "wss://relay.b", baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("insert relay b: %v", err)
	}
	if err := s.InsertCanonicalEvent(ctx, event, nil, "wss://relay.a", baseTime.Add(1*time.Minute)); err != nil {
		t.Fatalf("insert relay a: %v", err)
	}

	relays, err := s.GetEventSeenOn(ctx, event.ID)
	if err != nil {
		t.Fatalf("get seen-on: %v", err)
	}
	if len(relays) != 2 {
		t.Fatalf("expected 2 seen-on rows, got %d", len(relays))
	}
	if relays[0].RelayURL != "wss://relay.a" || relays[1].RelayURL != "wss://relay.b" {
		t.Fatalf("expected seen-on sorted by seen_at asc, got %#v", relays)
	}

	_, err = s.GetEventSeenOn(ctx, "missing_event")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing event, got %v", err)
	}
}
