package store

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestGetAuthorSentZaps_FiltersBySenderAndPaginates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	sender := "sender_zapper"
	receiver := "receiver_creator"
	targetNote := "target_note_1"

	mustInsertEventRow(t, pool, targetNote, receiver, 1000, 1)
	mustInsertZapReceipt(t, pool, "zap_newest", sender, receiver, targetNote, 21, 2002)
	mustInsertZapReceipt(t, pool, "zap_middle", sender, receiver, targetNote, 5, 2001)
	mustInsertZapReceipt(t, pool, "zap_oldest", sender, receiver, targetNote, 1, 2000)
	mustInsertZapReceipt(t, pool, "zap_received_only", receiver, sender, targetNote, 100, 1999)

	firstPage, next, err := s.GetAuthorSentZaps(ctx, sender, 2, nil)
	if err != nil {
		t.Fatalf("get first sent zaps page: %v", err)
	}
	if next == nil {
		t.Fatalf("expected next cursor on first page")
	}
	firstIDs := decodeZapIDs(t, firstPage)
	if !reflect.DeepEqual(firstIDs, []string{"zap_newest", "zap_middle"}) {
		t.Fatalf("unexpected first page ordering: got=%v", firstIDs)
	}

	secondPage, next2, err := s.GetAuthorSentZaps(ctx, sender, 2, next)
	if err != nil {
		t.Fatalf("get second sent zaps page: %v", err)
	}
	if next2 != nil {
		t.Fatalf("expected no next cursor on final page")
	}
	secondIDs := decodeZapIDs(t, secondPage)
	if !reflect.DeepEqual(secondIDs, []string{"zap_oldest"}) {
		t.Fatalf("unexpected second page ordering: got=%v", secondIDs)
	}

	receiverPage, _, err := s.GetAuthorSentZaps(ctx, receiver, 10, nil)
	if err != nil {
		t.Fatalf("get receiver sent zaps page: %v", err)
	}
	if len(receiverPage) != 0 {
		t.Fatalf("expected empty sent zaps for receiver-only profile, got %d", len(receiverPage))
	}
}

func TestGetAuthorReactions_FiltersByReactorAndPaginates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	reactor := "reactor_pk"
	targetNote := "note_for_reaction"

	mustInsertEventRow(t, pool, targetNote, "author_pk", 1000, 1)
	mustInsertEventRow(t, pool, "react_b", reactor, 2002, 7)
	mustInsertEventRow(t, pool, "react_a", reactor, 2001, 7)
	if _, err := pool.Exec(ctx, `
		INSERT INTO reaction_events (event_id, target_event_id, reactor_pubkey, content, created_at, derivation_version)
		VALUES
			('react_b', $1, $2, '+', 2002, 1),
			('react_a', $1, $2, '❤️', 2001, 1)
	`, targetNote, reactor); err != nil {
		t.Fatalf("insert reaction rows: %v", err)
	}

	firstPage, next, err := s.GetAuthorReactions(ctx, reactor, 1, nil)
	if err != nil {
		t.Fatalf("get first reactions page: %v", err)
	}
	if next == nil {
		t.Fatalf("expected next cursor on first page")
	}
	if got := decodeReactionIDs(t, firstPage); !reflect.DeepEqual(got, []string{"react_b"}) {
		t.Fatalf("unexpected first page: got=%v", got)
	}

	secondPage, next2, err := s.GetAuthorReactions(ctx, reactor, 1, next)
	if err != nil {
		t.Fatalf("get second reactions page: %v", err)
	}
	if next2 != nil {
		t.Fatalf("expected no next cursor on final page")
	}
	if got := decodeReactionIDs(t, secondPage); !reflect.DeepEqual(got, []string{"react_a"}) {
		t.Fatalf("unexpected second page: got=%v", got)
	}
}

func decodeZapIDs(t *testing.T, rows []json.RawMessage) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		var payload struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(row, &payload); err != nil {
			t.Fatalf("decode zap row: %v", err)
		}
		out = append(out, payload.EventID)
	}
	return out
}

func decodeReactionIDs(t *testing.T, rows []json.RawMessage) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		var payload struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(row, &payload); err != nil {
			t.Fatalf("decode reaction row: %v", err)
		}
		out = append(out, payload.EventID)
	}
	return out
}
