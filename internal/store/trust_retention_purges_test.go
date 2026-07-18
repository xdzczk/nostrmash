package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertNoteCandidate(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string, minHops *int, projectedAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO trusted_note_discovery_candidates (event_id, author_pubkey, min_hops, projected_at)
		VALUES ($1, 'author', $2, $3)
	`, eventID, minHops, projectedAt.UTC()); err != nil {
		t.Fatalf("insert note candidate %s: %v", eventID, err)
	}
}

func insertAccountStateRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey, state string, manualOverride *string, lastObservedAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO account_states (pubkey, state, derived_state, manual_override, last_observed_at)
		VALUES ($1, $2, $2, $3, $4)
	`, pubkey, state, manualOverride, lastObservedAt.UTC()); err != nil {
		t.Fatalf("insert account state %s: %v", pubkey, err)
	}
}

func TestPurgeStaleTrustedDiscoveryCandidates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	trustedBefore := ref.Add(-24 * time.Hour)
	untrustedBefore := ref.Add(-6 * time.Hour)

	hops := 2
	// Deleted: qualified but stale past the trusted horizon.
	insertNoteCandidate(t, ctx, pool, "n_stale_trusted", &hops, trustedBefore.Add(-time.Hour))
	// Deleted: unqualified and stale past the untrusted horizon.
	insertNoteCandidate(t, ctx, pool, "n_stale_unqualified", nil, untrustedBefore.Add(-time.Hour))
	// Kept: qualified inside the trusted horizon (even though it is past the
	// untrusted one), and unqualified inside the untrusted horizon.
	insertNoteCandidate(t, ctx, pool, "n_fresh_trusted", &hops, untrustedBefore.Add(-time.Hour))
	insertNoteCandidate(t, ctx, pool, "n_fresh_unqualified", nil, ref.Add(-time.Hour))

	deleted, err := NewPostgresStore(pool).PurgeStaleTrustedDiscoveryCandidates(ctx, trustedBefore, untrustedBefore, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deletions, got %d", deleted)
	}

	rows, err := pool.Query(ctx, `SELECT event_id FROM trusted_note_discovery_candidates ORDER BY event_id ASC`)
	if err != nil {
		t.Fatalf("query candidates: %v", err)
	}
	defer rows.Close()
	var remaining []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		remaining = append(remaining, id)
	}
	want := []string{"n_fresh_trusted", "n_fresh_unqualified"}
	if len(remaining) != len(want) || remaining[0] != want[0] || remaining[1] != want[1] {
		t.Fatalf("remaining mismatch: got %v want %v", remaining, want)
	}
}

func TestPurgeIdleAccountStates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	trustedBefore := ref.Add(-12 * time.Hour)
	untrustedBefore := ref.Add(-3 * time.Hour)

	insertTrustSnapshotRow(t, ctx, pool, "trusted_idle")
	insertTrustSnapshotRow(t, ctx, pool, "trusted_fresh")

	override := "observed"
	// Deleted: idle untrusted 'unknown' account, idle trusted 'observed' account.
	insertAccountStateRow(t, ctx, pool, "untrusted_idle", "unknown", nil, untrustedBefore.Add(-time.Hour))
	insertAccountStateRow(t, ctx, pool, "trusted_idle", "observed", nil, trustedBefore.Add(-time.Hour))
	// Kept: untrusted but within its horizon; trusted within its (longer)
	// horizon; promoted state; manual override.
	insertAccountStateRow(t, ctx, pool, "untrusted_fresh", "observed", nil, ref.Add(-time.Hour))
	insertAccountStateRow(t, ctx, pool, "trusted_fresh", "observed", nil, untrustedBefore.Add(-time.Hour))
	insertAccountStateRow(t, ctx, pool, "promoted", "candidate", nil, untrustedBefore.Add(-48*time.Hour))
	insertAccountStateRow(t, ctx, pool, "pinned", "observed", &override, untrustedBefore.Add(-48*time.Hour))

	deleted, err := NewPostgresStore(pool).PurgeIdleAccountStates(ctx, trustedBefore, untrustedBefore, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deletions, got %d", deleted)
	}

	rows, err := pool.Query(ctx, `SELECT pubkey FROM account_states ORDER BY pubkey ASC`)
	if err != nil {
		t.Fatalf("query account states: %v", err)
	}
	defer rows.Close()
	var remaining []string
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		remaining = append(remaining, pk)
	}
	want := []string{"pinned", "promoted", "trusted_fresh", "untrusted_fresh"}
	if len(remaining) != len(want) {
		t.Fatalf("remaining mismatch: got %v want %v", remaining, want)
	}
	for i := range want {
		if remaining[i] != want[i] {
			t.Fatalf("remaining mismatch: got %v want %v", remaining, want)
		}
	}
}
