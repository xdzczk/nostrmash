package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetAuthorRecentEvents_IncludesEngagementCounts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	note := model.Event{
		ID:          "author_counts_note",
		Pubkey:      "author_counts",
		CreatedAt:   now.Add(-2 * time.Hour).Unix(),
		Kind:        1,
		Content:     "hello",
		Sig:         "sig_note",
		FirstSeenAt: now.Add(-2 * time.Hour),
		InsertedAt:  now,
	}
	note.RawJSON, _ = json.Marshal(map[string]any{
		"id": note.ID, "pubkey": note.Pubkey, "created_at": note.CreatedAt,
		"kind": note.Kind, "tags": []any{}, "content": note.Content, "sig": note.Sig,
	})
	reply := model.Event{
		ID:          "author_counts_reply",
		Pubkey:      "author_counts_replyer",
		CreatedAt:   now.Add(-1 * time.Hour).Unix(),
		Kind:        1,
		Content:     "reply",
		Sig:         "sig_reply",
		FirstSeenAt: now.Add(-1 * time.Hour),
		InsertedAt:  now,
	}
	reply.RawJSON, _ = json.Marshal(map[string]any{
		"id": reply.ID, "pubkey": reply.Pubkey, "created_at": reply.CreatedAt,
		"kind": reply.Kind, "tags": [][]string{{"e", note.ID, "", "reply"}}, "content": reply.Content, "sig": reply.Sig,
	})
	reaction := model.Event{
		ID:          "author_counts_reaction",
		Pubkey:      "author_counts_reactor",
		CreatedAt:   now.Add(-30 * time.Minute).Unix(),
		Kind:        7,
		Content:     "+",
		Sig:         "sig_reaction",
		FirstSeenAt: now.Add(-30 * time.Minute),
		InsertedAt:  now,
	}
	reaction.RawJSON, _ = json.Marshal(map[string]any{
		"id": reaction.ID, "pubkey": reaction.Pubkey, "created_at": reaction.CreatedAt,
		"kind": reaction.Kind, "tags": [][]string{{"e", note.ID}}, "content": reaction.Content, "sig": reaction.Sig,
	})

	for _, event := range []model.Event{note, reply, reaction} {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := s.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive %s: %v", event.ID, err)
		}
	}

	events, err := s.GetAuthorRecentEvents(ctx, note.Pubkey, 10)
	if err != nil {
		t.Fatalf("GetAuthorRecentEvents: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one author event")
	}
	var payload map[string]any
	if err := json.Unmarshal(events[0], &payload); err != nil {
		t.Fatalf("decode enriched event: %v", err)
	}
	if payload["id"] != note.ID {
		t.Fatalf("unexpected first event id: %#v", payload["id"])
	}
	if payload["reply_count"] != float64(1) {
		t.Fatalf("unexpected reply_count: %#v", payload["reply_count"])
	}
	if payload["reaction_count"] != float64(1) {
		t.Fatalf("unexpected reaction_count: %#v", payload["reaction_count"])
	}
	counts, ok := payload["counts"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested counts object, got %#v", payload["counts"])
	}
	if counts["reply_count"] != float64(1) || counts["reaction_count"] != float64(1) {
		t.Fatalf("unexpected nested counts: %#v", counts)
	}
}
