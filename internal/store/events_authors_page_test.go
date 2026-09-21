package store

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func mustInsertAuthorRecentEvent(t *testing.T, pool *pgxpool.Pool, id, pubkey string, createdAt int64, kind int) {
	t.Helper()
	mustInsertEventRow(t, pool, id, pubkey, createdAt, kind)
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO author_recent_events (author_pubkey, event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, 1)
	`, pubkey, id, createdAt); err != nil {
		t.Fatalf("insert author recent row %s: %v", id, err)
	}
}

func decodeAuthorEventIDs(t *testing.T, rows []json.RawMessage) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(row, &payload); err != nil {
			t.Fatalf("decode author event row: %v", err)
		}
		out = append(out, payload.ID)
	}
	return out
}

func TestGetAuthorRecentEventsPage_PaginatesWithoutGapsOrDuplicates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	author := "author_page_pk"
	other := "other_author_pk"

	mustInsertAuthorRecentEvent(t, pool, "evt_newest", author, 3000, 1)
	// Tie on created_at resolved by event_id desc.
	mustInsertAuthorRecentEvent(t, pool, "evt_tie_b", author, 2000, 1)
	mustInsertAuthorRecentEvent(t, pool, "evt_tie_a", author, 2000, 6)
	mustInsertAuthorRecentEvent(t, pool, "evt_oldest", author, 1000, 1)
	mustInsertAuthorRecentEvent(t, pool, "evt_other_author", other, 2500, 1)

	firstPage, next, err := s.GetAuthorRecentEventsPage(ctx, author, 2, nil)
	if err != nil {
		t.Fatalf("get first author events page: %v", err)
	}
	if next == nil {
		t.Fatalf("expected next cursor on first page")
	}
	if got := decodeAuthorEventIDs(t, firstPage); !reflect.DeepEqual(got, []string{"evt_newest", "evt_tie_b"}) {
		t.Fatalf("unexpected first page: got=%v", got)
	}

	secondPage, next2, err := s.GetAuthorRecentEventsPage(ctx, author, 2, next)
	if err != nil {
		t.Fatalf("get second author events page: %v", err)
	}
	if next2 != nil {
		t.Fatalf("expected no next cursor on final page")
	}
	if got := decodeAuthorEventIDs(t, secondPage); !reflect.DeepEqual(got, []string{"evt_tie_a", "evt_oldest"}) {
		t.Fatalf("unexpected second page: got=%v", got)
	}
}

func TestGetAuthorRecentEventsPage_ExactPageBoundaryOmitsNextCursor(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	author := "author_boundary_pk"
	mustInsertAuthorRecentEvent(t, pool, "evt_bound_2", author, 2000, 1)
	mustInsertAuthorRecentEvent(t, pool, "evt_bound_1", author, 1000, 1)

	page, next, err := s.GetAuthorRecentEventsPage(ctx, author, 2, nil)
	if err != nil {
		t.Fatalf("get author events page: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("unexpected page size: %d", len(page))
	}
	if next != nil {
		t.Fatalf("expected no next cursor when the page consumed every row")
	}
}

func TestGetAuthorRecentEventsByKindPage_FiltersKindAndPaginates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	author := "author_kind_page_pk"
	mustInsertAuthorRecentEvent(t, pool, "note_new", author, 3000, 1)
	mustInsertAuthorRecentEvent(t, pool, "repost_mid", author, 2500, 6)
	mustInsertAuthorRecentEvent(t, pool, "note_mid", author, 2000, 1)
	mustInsertAuthorRecentEvent(t, pool, "note_old", author, 1000, 1)

	firstPage, next, err := s.GetAuthorRecentEventsByKindPage(ctx, author, 1, 2, nil)
	if err != nil {
		t.Fatalf("get first kind page: %v", err)
	}
	if next == nil {
		t.Fatalf("expected next cursor on first kind page")
	}
	if got := decodeAuthorEventIDs(t, firstPage); !reflect.DeepEqual(got, []string{"note_new", "note_mid"}) {
		t.Fatalf("unexpected first kind page: got=%v", got)
	}

	secondPage, next2, err := s.GetAuthorRecentEventsByKindPage(ctx, author, 1, 2, next)
	if err != nil {
		t.Fatalf("get second kind page: %v", err)
	}
	if next2 != nil {
		t.Fatalf("expected no next cursor on final kind page")
	}
	if got := decodeAuthorEventIDs(t, secondPage); !reflect.DeepEqual(got, []string{"note_old"}) {
		t.Fatalf("unexpected second kind page: got=%v", got)
	}
}
