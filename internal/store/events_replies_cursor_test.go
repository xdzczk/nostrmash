package store

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetEventReplies_CursorStableAcrossCreatedAtTies(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	baseTime := time.Date(2026, 4, 4, 17, 0, 0, 0, time.UTC)

	parent := model.Event{
		ID:          "reply_parent",
		Pubkey:      "author_parent",
		CreatedAt:   2000,
		Kind:        1,
		Sig:         "sig_parent",
		Content:     "parent",
		RawJSON:     json.RawMessage(`{"id":"reply_parent","kind":1,"tags":[]}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	if err := s.InsertCanonicalEvent(ctx, parent, nil, "wss://relay.one", parent.FirstSeenAt); err != nil {
		t.Fatalf("insert parent event: %v", err)
	}

	replies := []model.Event{
		{
			ID:          "reply_a",
			Pubkey:      "author_a",
			CreatedAt:   2001,
			Kind:        1,
			Sig:         "sig_a",
			Content:     "a",
			RawJSON:     json.RawMessage(`{"id":"reply_a","kind":1,"tags":[["e","reply_parent","","reply"]]}`),
			FirstSeenAt: baseTime.Add(1 * time.Second),
			InsertedAt:  baseTime.Add(1 * time.Second),
		},
		{
			ID:          "reply_b",
			Pubkey:      "author_b",
			CreatedAt:   2001,
			Kind:        1,
			Sig:         "sig_b",
			Content:     "b",
			RawJSON:     json.RawMessage(`{"id":"reply_b","kind":1,"tags":[["e","reply_parent","","reply"]]}`),
			FirstSeenAt: baseTime.Add(2 * time.Second),
			InsertedAt:  baseTime.Add(2 * time.Second),
		},
		{
			ID:          "reply_c",
			Pubkey:      "author_c",
			CreatedAt:   2002,
			Kind:        1,
			Sig:         "sig_c",
			Content:     "c",
			RawJSON:     json.RawMessage(`{"id":"reply_c","kind":1,"tags":[["e","reply_parent","","reply"]]}`),
			FirstSeenAt: baseTime.Add(3 * time.Second),
			InsertedAt:  baseTime.Add(3 * time.Second),
		},
	}
	for _, reply := range replies {
		tags := [][]string{{"e", "reply_parent", "", "reply"}}
		if err := s.InsertCanonicalEvent(ctx, reply, tags, "wss://relay.one", reply.FirstSeenAt); err != nil {
			t.Fatalf("insert reply %s: %v", reply.ID, err)
		}
		if err := handlers.UpdateThreadProjection(ctx, reply.ID); err != nil {
			t.Fatalf("project thread edge for %s: %v", reply.ID, err)
		}
	}

	firstPage, next, err := s.GetEventReplies(ctx, parent.ID, 2, nil)
	if err != nil {
		t.Fatalf("get first replies page: %v", err)
	}
	if next == nil {
		t.Fatalf("expected next cursor for first page")
	}
	firstIDs := decodeEventIDs(t, firstPage)
	if !reflect.DeepEqual(firstIDs, []string{"reply_a", "reply_b"}) {
		t.Fatalf("unexpected first page ordering: got=%v", firstIDs)
	}

	secondPage, next2, err := s.GetEventReplies(ctx, parent.ID, 2, next)
	if err != nil {
		t.Fatalf("get second replies page: %v", err)
	}
	if next2 != nil {
		t.Fatalf("expected no next cursor on final page")
	}
	secondIDs := decodeEventIDs(t, secondPage)
	if !reflect.DeepEqual(secondIDs, []string{"reply_c"}) {
		t.Fatalf("unexpected second page ordering: got=%v", secondIDs)
	}
}
