package store

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertAppliedStatDeltaRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID, projection string, appliedAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO applied_stat_deltas (event_id, projection, applied_at)
		VALUES ($1, $2, $3)
	`, eventID, projection, appliedAt.UTC()); err != nil {
		t.Fatalf("insert applied stat delta %s/%s: %v", eventID, projection, err)
	}
}

func remainingAppliedStatDeltas(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT event_id || '|' || projection FROM applied_stat_deltas ORDER BY 1 ASC`)
	if err != nil {
		t.Fatalf("query applied stat deltas: %v", err)
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

// TestPruneOrphanedAppliedStatDeltas verifies the pruning job only reclaims
// ledger rows whose source event no longer exists, and only once past the
// grace-period cutoff — a live event's ledger row must never be pruned
// early, since a future retention purge may still need it to gate a
// decrement (see the doc comment on PruneOrphanedAppliedStatDeltas).
func TestPruneOrphanedAppliedStatDeltas(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	appliedBefore := ref.Add(-1 * time.Hour)
	ancient := appliedBefore.Add(-time.Hour)

	// e_live's event row exists: its ledger row must survive even though it
	// is well past the grace cutoff.
	insertEventRow(t, ctx, pool, "e_live", 1, ref.Unix())
	insertAppliedStatDeltaRow(t, ctx, pool, "e_live", "profile_public_stats", ancient)

	// e_deleted_old has no matching event row and is past the cutoff: a pure
	// orphan, eligible for pruning.
	insertAppliedStatDeltaRow(t, ctx, pool, "e_deleted_old", "profile_public_stats", ancient)
	insertAppliedStatDeltaRow(t, ctx, pool, "e_deleted_old", "author_activity_daily", ancient)

	// e_deleted_recent also has no matching event row, but is inside the
	// grace period: must survive this pass regardless of orphan status.
	insertAppliedStatDeltaRow(t, ctx, pool, "e_deleted_recent", "profile_public_stats", ref)

	deleted, err := NewPostgresStore(pool).PruneOrphanedAppliedStatDeltas(ctx, appliedBefore, 100)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deletions, got %d", deleted)
	}

	got := remainingAppliedStatDeltas(t, ctx, pool)
	want := []string{
		"e_deleted_recent|profile_public_stats",
		"e_live|profile_public_stats",
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
