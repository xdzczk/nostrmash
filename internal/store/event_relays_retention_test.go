package store

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertEventRelayRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID, relayURL string, seenAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_relays (event_id, relay_url, seen_at, pubkey)
		VALUES ($1, $2, $3, 'pub')
	`, eventID, relayURL, seenAt.UTC()); err != nil {
		t.Fatalf("insert event relay %s/%s: %v", eventID, relayURL, err)
	}
}

func remainingEventRelays(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT event_id || '|' || relay_url FROM event_relays ORDER BY 1 ASC`)
	if err != nil {
		t.Fatalf("query event relays: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatalf("scan key: %v", err)
		}
		out = append(out, key)
	}
	return out
}

func TestPurgeStaleEventRelays(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	seenBefore := ref.Add(-180 * 24 * time.Hour)
	ancient := seenBefore.Add(-24 * time.Hour)
	recent := ref

	insertEventRow(t, ctx, pool, "e_multi", 1, ref.Unix())
	insertEventRow(t, ctx, pool, "e_single", 1, ref.Unix())
	insertEventRow(t, ctx, pool, "e_ties", 1, ref.Unix())

	// e_multi: earliest ancient row survives, later ancient dup goes, recent dup stays.
	insertEventRelayRow(t, ctx, pool, "e_multi", "wss://first", ancient.Add(-time.Hour))
	insertEventRelayRow(t, ctx, pool, "e_multi", "wss://dup_old", ancient)
	insertEventRelayRow(t, ctx, pool, "e_multi", "wss://dup_recent", recent)

	// e_single: sole ancient row is first-provenance and survives.
	insertEventRelayRow(t, ctx, pool, "e_single", "wss://only", ancient)

	// e_ties: equal seen_at, relay_url breaks the tie (lower URL survives).
	insertEventRelayRow(t, ctx, pool, "e_ties", "wss://a", ancient)
	insertEventRelayRow(t, ctx, pool, "e_ties", "wss://b", ancient)

	deleted, err := NewPostgresStore(pool).PurgeStaleEventRelays(ctx, seenBefore, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deletions, got %d", deleted)
	}

	got := remainingEventRelays(t, ctx, pool)
	want := []string{
		"e_multi|wss://dup_recent",
		"e_multi|wss://first",
		"e_single|wss://only",
		"e_ties|wss://a",
	}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("remaining mismatch: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("remaining mismatch: got %v want %v", got, want)
		}
	}
}
