package store

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertEventRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, kind int, createdAt int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, 'pub', $2, $3, 'sig', '', '{}'::jsonb)
	`, id, createdAt, kind); err != nil {
		t.Fatalf("insert event %s: %v", id, err)
	}
}

func insertDeriveJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID, status string, updatedAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO jobs (job_type, idempotency_key, status, updated_at)
		VALUES ('derive_event_bundle', $1, $2, $3)
	`, "derive_event_bundle:"+eventID, status, updatedAt.UTC()); err != nil {
		t.Fatalf("insert job for %s: %v", eventID, err)
	}
}

func remainingEventIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT id FROM events ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("query remaining events: %v", err)
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

func TestPurgeExpiredEngagementEvents_DerivationSafeGuard(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	createdBefore := ref
	deadGraceBefore := ref.Add(-7 * 24 * time.Hour)

	oldUnix := ref.Add(-10 * 24 * time.Hour).Unix()
	recentUnix := ref.Add(24 * time.Hour).Unix()

	// Purgeable: old engagement events with no blocking derivation.
	insertEventRow(t, ctx, pool, "e_old_no_job", 7, oldUnix)
	insertEventRow(t, ctx, pool, "e_old_dead_stale", 6, oldUnix)
	insertDeriveJob(t, ctx, pool, "e_old_dead_stale", "dead", deadGraceBefore.Add(-24*time.Hour))

	// Blocked: in-flight or recently-dead derivation.
	insertEventRow(t, ctx, pool, "e_old_pending", 6, oldUnix)
	insertDeriveJob(t, ctx, pool, "e_old_pending", "pending", ref)
	insertEventRow(t, ctx, pool, "e_old_running", 7, oldUnix)
	insertDeriveJob(t, ctx, pool, "e_old_running", "running", ref)
	insertEventRow(t, ctx, pool, "e_old_dead_recent", 9735, oldUnix)
	insertDeriveJob(t, ctx, pool, "e_old_dead_recent", "dead", deadGraceBefore.Add(24*time.Hour))

	// Kept: too recent, or not an engagement kind.
	insertEventRow(t, ctx, pool, "e_recent_no_job", 7, recentUnix)
	insertEventRow(t, ctx, pool, "e_kind1_old", 1, oldUnix)

	deleted, err := NewPostgresStore(pool).PurgeExpiredEngagementEvents(ctx, createdBefore, deadGraceBefore, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deletions, got %d", deleted)
	}

	got := remainingEventIDs(t, ctx, pool)
	want := []string{"e_kind1_old", "e_old_dead_recent", "e_old_pending", "e_old_running", "e_recent_no_job"}
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
