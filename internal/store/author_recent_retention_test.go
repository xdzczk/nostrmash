package store

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertAuthorRecentRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, author, eventID string, createdAt int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, $2, $3, 1, 'sig', '', '{}'::jsonb)
	`, eventID, author, createdAt); err != nil {
		t.Fatalf("insert event %s: %v", eventID, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO author_recent_events (author_pubkey, event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, 1)
	`, author, eventID, createdAt); err != nil {
		t.Fatalf("insert author recent row %s: %v", eventID, err)
	}
}

func remainingAuthorRecentIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT event_id FROM author_recent_events ORDER BY event_id ASC`)
	if err != nil {
		t.Fatalf("query author recent rows: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		out = append(out, id)
	}
	return out
}

func TestPruneAuthorRecentEvents(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	olderThan := ref.Add(-90 * 24 * time.Hour)

	// Author A: 5 recent rows; with cap 3 the 2 oldest must go.
	for i := 0; i < 5; i++ {
		createdAt := ref.Add(-time.Duration(i) * time.Hour).Unix()
		insertAuthorRecentRow(t, ctx, pool, "author_a", fmt.Sprintf("a_%d", i), createdAt)
	}
	// Author B: 2 recent rows (under cap) and 1 ancient row (age pass).
	insertAuthorRecentRow(t, ctx, pool, "author_b", "b_recent_0", ref.Add(-1*time.Hour).Unix())
	insertAuthorRecentRow(t, ctx, pool, "author_b", "b_recent_1", ref.Add(-2*time.Hour).Unix())
	insertAuthorRecentRow(t, ctx, pool, "author_b", "b_ancient", olderThan.Add(-24*time.Hour).Unix())

	deleted, err := NewPostgresStore(pool).PruneAuthorRecentEvents(ctx, olderThan, 3, 10, 100)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("expected 3 deletions (1 age + 2 cap), got %d", deleted)
	}

	got := remainingAuthorRecentIDs(t, ctx, pool)
	want := []string{"a_0", "a_1", "a_2", "b_recent_0", "b_recent_1"}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("remaining mismatch: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("remaining mismatch: got %v want %v", got, want)
		}
	}

	// Canonical events must be untouched: only the projection is pruned.
	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 8 {
		t.Fatalf("expected all 8 canonical events to survive, got %d", eventCount)
	}
}
