package derivation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestUpdateReplaceableState_TieBreakByEventID(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 16, 30, 0, 0, time.UTC)
	events := []model.Event{
		{
			ID:          "aaaaaaaa",
			Pubkey:      "pub_replaceable",
			CreatedAt:   1000,
			Kind:        0,
			Sig:         "sig_a",
			Content:     "a",
			RawJSON:     json.RawMessage(`{"id":"aaaaaaaa","kind":0,"tags":[]}`),
			FirstSeenAt: baseTime,
			InsertedAt:  baseTime,
		},
		{
			ID:          "bbbbbbbb",
			Pubkey:      "pub_replaceable",
			CreatedAt:   1000,
			Kind:        0,
			Sig:         "sig_b",
			Content:     "b",
			RawJSON:     json.RawMessage(`{"id":"bbbbbbbb","kind":0,"tags":[]}`),
			FirstSeenAt: baseTime.Add(1 * time.Second),
			InsertedAt:  baseTime.Add(1 * time.Second),
		},
		{
			ID:          "00000000",
			Pubkey:      "pub_replaceable",
			CreatedAt:   1001,
			Kind:        0,
			Sig:         "sig_c",
			Content:     "c",
			RawJSON:     json.RawMessage(`{"id":"00000000","kind":0,"tags":[]}`),
			FirstSeenAt: baseTime.Add(2 * time.Second),
			InsertedAt:  baseTime.Add(2 * time.Second),
		},
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, nil, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}

	if err := handlers.UpdateReplaceableState(ctx, "aaaaaaaa"); err != nil {
		t.Fatalf("derive replaceable state a: %v", err)
	}
	if err := handlers.UpdateReplaceableState(ctx, "bbbbbbbb"); err != nil {
		t.Fatalf("derive replaceable state b: %v", err)
	}
	// Lower event id with same created_at should not replace bbbbbbbb.
	if err := handlers.UpdateReplaceableState(ctx, "aaaaaaaa"); err != nil {
		t.Fatalf("derive replaceable state a again: %v", err)
	}

	var winnerID string
	var winnerCreatedAt int64
	if err := pool.QueryRow(ctx, `
		SELECT event_id, created_at
		FROM replaceable_state
		WHERE pubkey = $1 AND kind = $2 AND d_tag = ''
	`, "pub_replaceable", 0).Scan(&winnerID, &winnerCreatedAt); err != nil {
		t.Fatalf("query replaceable winner after tie: %v", err)
	}
	if winnerID != "bbbbbbbb" || winnerCreatedAt != 1000 {
		t.Fatalf("unexpected winner after tie-break: id=%s created_at=%d", winnerID, winnerCreatedAt)
	}

	if err := handlers.UpdateReplaceableState(ctx, "00000000"); err != nil {
		t.Fatalf("derive replaceable state newer event: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT event_id, created_at
		FROM replaceable_state
		WHERE pubkey = $1 AND kind = $2 AND d_tag = ''
	`, "pub_replaceable", 0).Scan(&winnerID, &winnerCreatedAt); err != nil {
		t.Fatalf("query replaceable winner after newer: %v", err)
	}
	if winnerID != "00000000" || winnerCreatedAt != 1001 {
		t.Fatalf("unexpected winner after newer event: id=%s created_at=%d", winnerID, winnerCreatedAt)
	}
}
