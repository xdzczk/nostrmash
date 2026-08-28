package store

import (
	"context"
	"sort"
	"testing"
	"time"
)

func TestPurgeOrphanDeletionLedger_KeeperVsOrphanSemantics(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	createdBefore := ref.Add(-90 * 24 * time.Hour)

	oldUnix := createdBefore.Add(-24 * time.Hour).Unix()
	recentUnix := createdBefore.Add(24 * time.Hour).Unix()

	// The stored target: tombstones pointing at it are keepers at any age.
	insertEventRow(t, ctx, pool, "stored_target", 1, oldUnix)

	// Orphan + old: purged.
	insertDeletionLedgerRow(t, ctx, pool, "tomb_orphan_old", "missing_target_a", oldUnix)
	// Orphan + old, second row to exercise multi-delete in one window.
	insertDeletionLedgerRow(t, ctx, pool, "tomb_orphan_old2", "missing_target_b", oldUnix+1)
	// Keeper + old: target stored, survives regardless of age.
	insertDeletionLedgerRow(t, ctx, pool, "tomb_keeper_old", "stored_target", oldUnix)
	// Orphan + recent: newer than the horizon, survives.
	insertDeletionLedgerRow(t, ctx, pool, "tomb_orphan_recent", "missing_target_c", recentUnix)

	st := NewPostgresStore(pool)

	// Walk the whole eligible range with a window of 1 to exercise the
	// composite cursor advancing past keepers, including a keeper and an
	// orphan sharing the same created_at second.
	var cursorCreatedAt int64
	var cursorEventID string
	var totalDeleted, totalScanned int64
	for {
		scanned, deleted, lastCreatedAt, lastEventID, err := st.PurgeOrphanDeletionLedger(ctx, cursorCreatedAt, cursorEventID, createdBefore, 1)
		if err != nil {
			t.Fatalf("purge window: %v", err)
		}
		totalScanned += scanned
		totalDeleted += deleted
		if scanned < 1 {
			break
		}
		cursorCreatedAt, cursorEventID = lastCreatedAt, lastEventID
	}

	if totalDeleted != 2 {
		t.Fatalf("expected 2 orphan tombstones deleted, got %d (scanned %d)", totalDeleted, totalScanned)
	}

	got := deletionLedgerEventIDs(t, ctx, pool)
	want := []string{"tomb_keeper_old", "tomb_orphan_recent"}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("remaining ledger mismatch: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("remaining ledger mismatch: got %v want %v", got, want)
		}
	}

	// An empty catchup pass over the same range scans only the surviving
	// old keeper (bounded work), deletes nothing.
	scanned, deleted, _, _, err := st.PurgeOrphanDeletionLedger(ctx, 0, "", createdBefore, 100)
	if err != nil {
		t.Fatalf("empty catchup: %v", err)
	}
	if deleted != 0 || scanned != 1 {
		t.Fatalf("expected empty catchup scanned=1 deleted=0, got scanned=%d deleted=%d", scanned, deleted)
	}
}
