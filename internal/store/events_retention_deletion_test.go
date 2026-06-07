package store

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertDeletionLedgerRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID, target string, createdAt int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		VALUES ($1, 'pub', $2, $3, 1)
	`, eventID, target, createdAt); err != nil {
		t.Fatalf("insert deletion ledger row %s: %v", eventID, err)
	}
}

func deletionLedgerEventIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT event_id FROM deletion_events ORDER BY event_id ASC`)
	if err != nil {
		t.Fatalf("query deletion ledger: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan ledger id: %v", err)
		}
		out = append(out, id)
	}
	return out
}

func TestPurgeProcessedDeletionEvents_PreservesLedgerAndGuards(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	createdBefore := ref
	deadGraceBefore := ref.Add(-7 * 24 * time.Hour)

	oldUnix := ref.Add(-10 * 24 * time.Hour).Unix()
	recentUnix := ref.Add(24 * time.Hour).Unix()

	// Purgeable: old, projected, no blocking job.
	insertEventRow(t, ctx, pool, "d_old_no_job", 5, oldUnix)
	insertDeletionLedgerRow(t, ctx, pool, "d_old_no_job", "target_a", oldUnix)
	// Purgeable: old, dead derivation past grace.
	insertEventRow(t, ctx, pool, "d_old_dead_stale", 5, oldUnix)
	insertDeletionLedgerRow(t, ctx, pool, "d_old_dead_stale", "target_b", oldUnix)
	insertDeriveJob(t, ctx, pool, "d_old_dead_stale", "dead", deadGraceBefore.Add(-24*time.Hour))

	// Blocked: in-flight or recently-dead derivation.
	insertEventRow(t, ctx, pool, "d_old_pending", 5, oldUnix)
	insertDeriveJob(t, ctx, pool, "d_old_pending", "pending", ref)
	insertEventRow(t, ctx, pool, "d_old_dead_recent", 5, oldUnix)
	insertDeriveJob(t, ctx, pool, "d_old_dead_recent", "dead", deadGraceBefore.Add(24*time.Hour))

	// Kept: too recent, or not a deletion kind.
	insertEventRow(t, ctx, pool, "d_recent_no_job", 5, recentUnix)
	insertEventRow(t, ctx, pool, "k1_old", 1, oldUnix)

	deleted, err := NewPostgresStore(pool).PurgeProcessedDeletionEvents(ctx, createdBefore, deadGraceBefore, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deletions, got %d", deleted)
	}

	got := remainingEventIDs(t, ctx, pool)
	want := []string{"d_old_dead_recent", "d_old_pending", "d_recent_no_job", "k1_old"}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("remaining events mismatch: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("remaining events mismatch: got %v want %v", got, want)
		}
	}

	// The tombstone ledger rows for purged raw events must survive (migration
	// 000050 dropped the FK cascade).
	ledger := deletionLedgerEventIDs(t, ctx, pool)
	wantLedger := []string{"d_old_dead_stale", "d_old_no_job"}
	if len(ledger) != len(wantLedger) {
		t.Fatalf("ledger mismatch: got %v want %v", ledger, wantLedger)
	}
	for i := range wantLedger {
		if ledger[i] != wantLedger[i] {
			t.Fatalf("ledger mismatch: got %v want %v", ledger, wantLedger)
		}
	}
}
