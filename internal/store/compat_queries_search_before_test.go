package store

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func mustInsertSearchableNote(t *testing.T, pool *pgxpool.Pool, id, pubkey string, createdAt int64, content string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       1,
		"tags":       [][]string{},
		"content":    content,
		"sig":        "sig_" + id,
	})
	if err != nil {
		t.Fatalf("marshal searchable note: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, $2, $3, 1, $4, $5, $6::jsonb)
	`, id, pubkey, createdAt, "sig_"+id, content, string(raw)); err != nil {
		t.Fatalf("insert searchable note %s: %v", id, err)
	}
}

func TestSearchNotesBefore_KeysetContinuesLatestSortWithoutOverlap(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	author := "search_before_pk"
	mustInsertSearchableNote(t, pool, "srch_newest", author, 4000, "keysetprobe newest")
	mustInsertSearchableNote(t, pool, "srch_high", author, 3000, "keysetprobe high")
	mustInsertSearchableNote(t, pool, "srch_mid", author, 2000, "keysetprobe mid")
	mustInsertSearchableNote(t, pool, "srch_old", author, 1000, "keysetprobe old")
	// Matching text but newer than the cursor: must not appear after it.
	mustInsertSearchableNote(t, pool, "srch_unrelated", author, 5000, "different topic entirely")

	firstPage, err := s.SearchNotes(ctx, "keysetprobe", "latest", nil, "", 2, 0)
	if err != nil {
		t.Fatalf("first latest search page: %v", err)
	}
	if got := decodeAuthorEventIDs(t, firstPage); !reflect.DeepEqual(got, []string{"srch_newest", "srch_high"}) {
		t.Fatalf("unexpected first latest page: got=%v", got)
	}

	secondPage, err := s.SearchNotesBefore(ctx, "keysetprobe", nil, "", 2, 3000, "srch_high")
	if err != nil {
		t.Fatalf("keyset continuation page: %v", err)
	}
	if got := decodeAuthorEventIDs(t, secondPage); !reflect.DeepEqual(got, []string{"srch_mid", "srch_old"}) {
		t.Fatalf("unexpected keyset continuation page: got=%v", got)
	}
}

func TestSearchNotesBefore_EmptyCursorFallsBackToFirstPage(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	mustInsertSearchableNote(t, pool, "srch_fb_1", "search_fb_pk", 2000, "fallbackprobe latest")
	mustInsertSearchableNote(t, pool, "srch_fb_2", "search_fb_pk", 1000, "fallbackprobe older")

	page, err := s.SearchNotesBefore(ctx, "fallbackprobe", nil, "", 5, 0, "")
	if err != nil {
		t.Fatalf("search notes before with empty cursor: %v", err)
	}
	if got := decodeAuthorEventIDs(t, page); !reflect.DeepEqual(got, []string{"srch_fb_1", "srch_fb_2"}) {
		t.Fatalf("unexpected fallback first page: got=%v", got)
	}
}
