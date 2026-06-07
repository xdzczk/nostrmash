package store

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertReplaceableEventRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, pubkey string, kind int, createdAt int64, firstSeen time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at)
		VALUES ($1, $2, $3, $4, 'sig', '', '{}'::jsonb, $5)
	`, id, pubkey, createdAt, kind, firstSeen.UTC()); err != nil {
		t.Fatalf("insert event %s: %v", id, err)
	}
}

func insertReplaceableWinner(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, kind int, eventID string, createdAt int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, $2, '', $3, $4, 1)
	`, pubkey, kind, eventID, createdAt); err != nil {
		t.Fatalf("insert replaceable_state winner %s: %v", eventID, err)
	}
}

func TestPurgeSupersededReplaceableEvents_SafeGuards(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	supersededBefore := ref
	deadGraceBefore := ref.Add(-7 * 24 * time.Hour)

	oldSeen := ref.Add(-48 * time.Hour)
	recentSeen := ref.Add(1 * time.Hour)

	// Pubkey A, kind 3: old superseded version with no blocking job -> purge.
	insertReplaceableEventRow(t, ctx, pool, "a3_win", "A", 3, 200, oldSeen)
	insertReplaceableEventRow(t, ctx, pool, "a3_old", "A", 3, 100, oldSeen)
	insertReplaceableWinner(t, ctx, pool, "A", 3, "a3_win", 200)

	// Pubkey B, kind 0: old superseded with a stale dead job (past grace) -> purge.
	insertReplaceableEventRow(t, ctx, pool, "b0_win", "B", 0, 200, oldSeen)
	insertReplaceableEventRow(t, ctx, pool, "b0_old", "B", 0, 100, oldSeen)
	insertDeriveJob(t, ctx, pool, "b0_old", "dead", deadGraceBefore.Add(-24*time.Hour))
	insertReplaceableWinner(t, ctx, pool, "B", 0, "b0_win", 200)

	// Pubkey C, kind 10002: superseded but blocked (pending job) and a
	// recently-ingested superseded version (within MinAge) -> both kept.
	insertReplaceableEventRow(t, ctx, pool, "c_win", "C", 10002, 300, oldSeen)
	insertReplaceableEventRow(t, ctx, pool, "c_pending", "C", 10002, 100, oldSeen)
	insertDeriveJob(t, ctx, pool, "c_pending", "pending", ref)
	insertReplaceableEventRow(t, ctx, pool, "c_recent", "C", 10002, 200, recentSeen)
	insertReplaceableWinner(t, ctx, pool, "C", 10002, "c_win", 300)

	// Pubkey D, kind 3: only the winner exists -> kept.
	insertReplaceableEventRow(t, ctx, pool, "d3_only", "D", 3, 100, oldSeen)
	insertReplaceableWinner(t, ctx, pool, "D", 3, "d3_only", 100)

	// Pubkey E, kind 3: a newer version exists but is not yet projected; the
	// recorded winner is the older one. The newer event must NOT be deleted.
	insertReplaceableEventRow(t, ctx, pool, "e3_old_proj", "E", 3, 100, oldSeen)
	insertReplaceableEventRow(t, ctx, pool, "e3_new", "E", 3, 200, oldSeen)
	insertReplaceableWinner(t, ctx, pool, "E", 3, "e3_old_proj", 100)

	// Non-replaceable kind that is "superseded"-shaped should be ignored.
	insertReplaceableEventRow(t, ctx, pool, "k1_old", "A", 1, 100, oldSeen)

	deleted, err := NewPostgresStore(pool).PurgeSupersededReplaceableEvents(ctx, supersededBefore, deadGraceBefore, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deletions, got %d", deleted)
	}

	got := remainingEventIDs(t, ctx, pool)
	want := []string{"a3_win", "b0_win", "c_win", "c_pending", "c_recent", "d3_only", "e3_old_proj", "e3_new", "k1_old"}
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
