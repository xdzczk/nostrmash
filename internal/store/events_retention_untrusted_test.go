package store

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertAuthoredEventRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, pubkey string, kind int, createdAt int64, firstSeenAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at)
		VALUES ($1, $2, $3, $4, 'sig', '', '{}'::jsonb, $5)
	`, id, pubkey, createdAt, kind, firstSeenAt.UTC()); err != nil {
		t.Fatalf("insert event %s: %v", id, err)
	}
}

func insertTrustSnapshotRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed)
		VALUES ($1, 1, false)
		ON CONFLICT (pubkey) DO NOTHING
	`, pubkey); err != nil {
		t.Fatalf("insert trust snapshot row %s: %v", pubkey, err)
	}
}

func TestPurgeUntrustedAuthorEvents(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	olderThan := ref
	deadGraceBefore := ref.Add(-7 * 24 * time.Hour)

	oldUnix := ref.Add(-30 * 24 * time.Hour).Unix()
	recentUnix := ref.Add(24 * time.Hour).Unix()
	oldSeen := ref.Add(-30 * 24 * time.Hour)
	recentSeen := ref.Add(24 * time.Hour)

	pg := NewPostgresStore(pool)

	// Fail-safe: with an empty trust_graph_snapshot nothing is deleted.
	insertAuthoredEventRow(t, ctx, pool, "u_failsafe", "untrusted_pub", 1, oldUnix, oldSeen)
	deleted, err := pg.PurgeUntrustedAuthorEvents(ctx, olderThan, deadGraceBefore, 100)
	if err != nil {
		t.Fatalf("purge with empty snapshot: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("empty trust graph must delete nothing, got %d", deleted)
	}

	insertTrustSnapshotRow(t, ctx, pool, "trusted_pub")

	// Purgeable: old author-gated kinds from an untrusted author.
	insertAuthoredEventRow(t, ctx, pool, "u_old_note", "untrusted_pub", 1, oldUnix, oldSeen)
	insertAuthoredEventRow(t, ctx, pool, "u_old_article", "untrusted_pub", 30023, oldUnix, oldSeen)
	insertAuthoredEventRow(t, ctx, pool, "u_old_deletion", "untrusted_pub", 5, oldUnix, oldSeen)

	// Kept: trusted author, open kind, too recent, backfilled recently, or
	// blocked by an in-flight derivation.
	insertAuthoredEventRow(t, ctx, pool, "k_trusted_note", "trusted_pub", 1, oldUnix, oldSeen)
	insertAuthoredEventRow(t, ctx, pool, "k_open_kind", "untrusted_pub", 0, oldUnix, oldSeen)
	insertAuthoredEventRow(t, ctx, pool, "k_recent_note", "untrusted_pub", 1, recentUnix, recentSeen)
	insertAuthoredEventRow(t, ctx, pool, "k_backfilled", "untrusted_pub", 1, oldUnix, recentSeen)
	insertAuthoredEventRow(t, ctx, pool, "k_pending_job", "untrusted_pub", 1, oldUnix, oldSeen)
	insertDeriveJob(t, ctx, pool, "k_pending_job", "pending", ref)

	deleted, err = pg.PurgeUntrustedAuthorEvents(ctx, olderThan, deadGraceBefore, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	// u_failsafe from the first phase is now eligible too, plus the
	// note/article/deletion trio above.
	if deleted != 4 {
		t.Fatalf("expected 4 deletions, got %d", deleted)
	}

	got := remainingEventIDs(t, ctx, pool)
	want := []string{"k_backfilled", "k_open_kind", "k_pending_job", "k_recent_note", "k_trusted_note"}
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
