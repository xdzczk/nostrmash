package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
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
	var payloadB map[string]any
	if err := json.Unmarshal(batch["event_query_b"], &payloadB); err != nil {
		t.Fatalf("decode batch event b: %v", err)
	}
	if payloadB["id"] != "event_query_b" || payloadB["kind"] != float64(1) {
		t.Fatalf("event b core fields mismatch: %#v", payloadB)
	}
	if _, ok := payloadB["reply_count"]; !ok {
		t.Fatalf("expected batch payloads to include engagement counts, got %#v", payloadB)
	}
}

func TestGetEventRawsByIDs_IncludesEngagementCounts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	note := model.Event{
		ID:          "batch_counts_note",
		Pubkey:      "batch_counts_author",
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
		ID:          "batch_counts_reply",
		Pubkey:      "batch_counts_replyer",
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
		ID:          "batch_counts_reaction",
		Pubkey:      "batch_counts_reactor",
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

	batch, err := s.GetEventRawsByIDs(ctx, []string{note.ID})
	if err != nil {
		t.Fatalf("GetEventRawsByIDs: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(batch[note.ID], &payload); err != nil {
		t.Fatalf("decode enriched batch event: %v", err)
	}
	if payload["reply_count"] != float64(1) {
		t.Fatalf("unexpected reply_count: %#v", payload["reply_count"])
	}
	if payload["reaction_count"] != float64(1) {
		t.Fatalf("unexpected reaction_count: %#v", payload["reaction_count"])
	}
}

func TestGetEventReplies_IncludesEngagementCounts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	root := model.Event{
		ID:          "thread_counts_root",
		Pubkey:      "thread_counts_root_author",
		CreatedAt:   now.Add(-3 * time.Hour).Unix(),
		Kind:        1,
		Content:     "root",
		Sig:         "sig_root",
		FirstSeenAt: now.Add(-3 * time.Hour),
		InsertedAt:  now,
	}
	root.RawJSON, _ = json.Marshal(map[string]any{
		"id": root.ID, "pubkey": root.Pubkey, "created_at": root.CreatedAt,
		"kind": root.Kind, "tags": []any{}, "content": root.Content, "sig": root.Sig,
	})
	reply := model.Event{
		ID:          "thread_counts_reply",
		Pubkey:      "thread_counts_reply_author",
		CreatedAt:   now.Add(-2 * time.Hour).Unix(),
		Kind:        1,
		Content:     "reply",
		Sig:         "sig_reply",
		FirstSeenAt: now.Add(-2 * time.Hour),
		InsertedAt:  now,
	}
	reply.RawJSON, _ = json.Marshal(map[string]any{
		"id": reply.ID, "pubkey": reply.Pubkey, "created_at": reply.CreatedAt,
		"kind": reply.Kind, "tags": [][]string{{"e", root.ID, "", "reply"}}, "content": reply.Content, "sig": reply.Sig,
	})
	nested := model.Event{
		ID:          "thread_counts_nested",
		Pubkey:      "thread_counts_nested_author",
		CreatedAt:   now.Add(-1 * time.Hour).Unix(),
		Kind:        1,
		Content:     "nested",
		Sig:         "sig_nested",
		FirstSeenAt: now.Add(-1 * time.Hour),
		InsertedAt:  now,
	}
	nested.RawJSON, _ = json.Marshal(map[string]any{
		"id": nested.ID, "pubkey": nested.Pubkey, "created_at": nested.CreatedAt,
		"kind": nested.Kind, "tags": [][]string{{"e", reply.ID, "", "reply"}}, "content": nested.Content, "sig": nested.Sig,
	})

	for _, event := range []model.Event{root, reply, nested} {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := s.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive %s: %v", event.ID, err)
		}
	}

	replies, _, err := s.GetEventReplies(ctx, root.ID, 10, nil)
	if err != nil {
		t.Fatalf("GetEventReplies: %v", err)
	}
	if len(replies) != 1 {
		t.Fatalf("expected 1 direct reply, got %d", len(replies))
	}
	var payload map[string]any
	if err := json.Unmarshal(replies[0], &payload); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if payload["id"] != reply.ID {
		t.Fatalf("unexpected reply id: %#v", payload["id"])
	}
	if payload["reply_count"] != float64(1) {
		t.Fatalf("unexpected reply_count on thread reply card: %#v", payload["reply_count"])
	}
}
