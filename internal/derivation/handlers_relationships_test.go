package derivation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestDeriveEventRelationships_UnmarkedV1Semantics(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	tags := [][]string{
		{"e", "event_root"},
		{"e", "event_mention"},
		{"e", "event_reply"},
		{"p", "pub_root"},
		{"p", "pub_mention"},
		{"p", "pub_reply"},
	}
	raw, err := json.Marshal(map[string]any{
		"id":         "source_event_1",
		"pubkey":     "source_pub",
		"created_at": 1000,
		"kind":       1,
		"tags":       tags,
		"content":    "hello",
		"sig":        "sig_source_1",
	})
	if err != nil {
		t.Fatalf("marshal source event raw json: %v", err)
	}
	event := model.Event{
		ID:          "source_event_1",
		Pubkey:      "source_pub",
		CreatedAt:   1000,
		Kind:        1,
		Sig:         "sig_source_1",
		Content:     "hello",
		RawJSON:     raw,
		FirstSeenAt: time.Date(2026, 4, 4, 16, 0, 0, 0, time.UTC),
		InsertedAt:  time.Date(2026, 4, 4, 16, 0, 0, 0, time.UTC),
	}
	if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert source event: %v", err)
	}

	if err := handlers.DeriveEventRelationships(ctx, event.ID); err != nil {
		t.Fatalf("derive event relationships: %v", err)
	}
	// Idempotency: a second run should converge to same rows.
	if err := handlers.DeriveEventRelationships(ctx, event.ID); err != nil {
		t.Fatalf("derive event relationships second run: %v", err)
	}

	eventRefRows, err := readEventRefRows(ctx, pool, event.ID)
	if err != nil {
		t.Fatalf("read event references: %v", err)
	}
	expectedEventRefs := []refRow{
		{referenced: "event_root", relation: "root", tagIndex: 0},
		{referenced: "event_mention", relation: "mention", tagIndex: 1},
		{referenced: "event_reply", relation: "reply", tagIndex: 2},
	}
	assertRefRowsEqual(t, eventRefRows, expectedEventRefs)

	pubkeyRefRows, err := readPubkeyRefRows(ctx, pool, event.ID)
	if err != nil {
		t.Fatalf("read pubkey references: %v", err)
	}
	expectedPubkeyRefs := []refRow{
		{referenced: "pub_root", relation: "root", tagIndex: 3},
		{referenced: "pub_mention", relation: "mention", tagIndex: 4},
		{referenced: "pub_reply", relation: "reply", tagIndex: 5},
	}
	assertRefRowsEqual(t, pubkeyRefRows, expectedPubkeyRefs)
}
