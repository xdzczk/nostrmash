package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectDMUnreadCounts_IdempotentAndDeletionAware(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	const sender = "sender_pubkey"
	const receiver = "receiver_pubkey"
	dm := newEventForTest(
		"dm_evt_1",
		sender,
		1000,
		4,
		[][]string{{"p", receiver}},
		`"encrypted"`,
		time.Unix(1000, 0).UTC(),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, dm, extractTagsFromRaw(t, dm.RawJSON), "wss://relay.one", dm.FirstSeenAt); err != nil {
		t.Fatalf("insert dm event: %v", err)
	}

	if err := handlers.ProjectDMUnreadCounts(ctx, dm.ID); err != nil {
		t.Fatalf("project dm unread first pass: %v", err)
	}
	if err := handlers.ProjectDMUnreadCounts(ctx, dm.ID); err != nil {
		t.Fatalf("project dm unread second pass: %v", err)
	}

	var pairCount int64
	if err := pool.QueryRow(ctx, `
		SELECT cnt
		FROM dm_unread_counts
		WHERE receiver_pubkey = $1
		  AND sender_pubkey = $2
	`, receiver, sender).Scan(&pairCount); err != nil {
		t.Fatalf("load pair dm count: %v", err)
	}
	if pairCount != 1 {
		t.Fatalf("expected pair count=1 after idempotent projection, got %d", pairCount)
	}

	deletion := newEventForTest(
		"del_evt_1",
		sender,
		1001,
		5,
		[][]string{{"e", dm.ID}},
		`""`,
		time.Unix(1001, 0).UTC(),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, deletion, extractTagsFromRaw(t, deletion.RawJSON), "wss://relay.one", deletion.FirstSeenAt); err != nil {
		t.Fatalf("insert deletion event: %v", err)
	}
	if err := handlers.DeriveEventRelationships(ctx, deletion.ID); err != nil {
		t.Fatalf("derive deletion references: %v", err)
	}
	if err := handlers.ProjectDeletionEvents(ctx, deletion.ID); err != nil {
		t.Fatalf("project deletion event: %v", err)
	}

	if err := pool.QueryRow(ctx, `
		SELECT cnt
		FROM dm_unread_counts
		WHERE receiver_pubkey = $1
		  AND sender_pubkey = $2
	`, receiver, sender).Scan(&pairCount); err != nil {
		t.Fatalf("load pair dm count after deletion: %v", err)
	}
	if pairCount != 0 {
		t.Fatalf("expected pair count=0 after deletion reconciliation, got %d", pairCount)
	}
}
