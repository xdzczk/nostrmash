package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestMarkProfileStatsDirty_SkipsKind3(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	contactList := newEventForTest(
		"skip_kind3_contacts",
		"alice",
		1000,
		3,
		[][]string{{"p", "bob"}, {"p", "carol"}},
		"",
		baseTime,
	)
	tags := extractTagsFromRaw(t, contactList.RawJSON)
	if err := pgStore.InsertCanonicalEvent(ctx, contactList, tags, "wss://relay.one", contactList.FirstSeenAt); err != nil {
		t.Fatalf("insert contact list: %v", err)
	}

	if err := handlers.MarkProfileStatsDirty(ctx, contactList.ID); err != nil {
		t.Fatalf("MarkProfileStatsDirty kind=3: %v", err)
	}
	var pendingAfterMark int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pending_profile_stats_recomputes
	`).Scan(&pendingAfterMark); err != nil {
		t.Fatalf("count pending after MarkProfileStatsDirty: %v", err)
	}
	if pendingAfterMark != 0 {
		t.Fatalf("expected MarkProfileStatsDirty to skip kind=3, got %d pending rows", pendingAfterMark)
	}

	if err := handlers.ProjectContactListsLatest(ctx, contactList.ID); err != nil {
		t.Fatalf("ProjectContactListsLatest: %v", err)
	}
	var pendingAfterContactLists int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pending_profile_stats_recomputes
	`).Scan(&pendingAfterContactLists); err != nil {
		t.Fatalf("count pending after ProjectContactListsLatest: %v", err)
	}
	if pendingAfterContactLists != 3 {
		t.Fatalf("expected contact-list projection to mark alice+bob+carol, got %d pending rows", pendingAfterContactLists)
	}
}

func TestDeriveEventBundle_Kind3MarksProfileStatsViaContactListsOnly(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 8, 1, 12, 5, 0, 0, time.UTC)

	contactList := newEventForTest(
		"bundle_kind3_contacts",
		"alice",
		1100,
		3,
		[][]string{{"p", "bob"}},
		"",
		baseTime,
	)
	tags := extractTagsFromRaw(t, contactList.RawJSON)
	if err := pgStore.InsertCanonicalEvent(ctx, contactList, tags, "wss://relay.one", contactList.FirstSeenAt); err != nil {
		t.Fatalf("insert contact list: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, contactList.ID); err != nil {
		t.Fatalf("DeriveEventBundle: %v", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT pubkey FROM pending_profile_stats_recomputes ORDER BY pubkey
	`)
	if err != nil {
		t.Fatalf("query pending profile stats: %v", err)
	}
	defer rows.Close()
	var pubkeys []string
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			t.Fatalf("scan pending pubkey: %v", err)
		}
		pubkeys = append(pubkeys, pubkey)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read pending pubkeys: %v", err)
	}
	if len(pubkeys) != 2 || pubkeys[0] != "alice" || pubkeys[1] != "bob" {
		t.Fatalf("unexpected pending pubkeys after kind=3 bundle: %v", pubkeys)
	}
}
