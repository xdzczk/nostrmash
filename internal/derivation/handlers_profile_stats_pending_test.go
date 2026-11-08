package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestGiftWrapDoesNotEnqueueOrRebuildStats(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	wrap := newEventForTest(
		"gift_wrap_skip",
		"ephemeral",
		1000,
		1059,
		[][]string{{"p", "alice"}},
		"ciphertext",
		baseTime,
	)
	tags := extractTagsFromRaw(t, wrap.RawJSON)
	if err := pgStore.InsertCanonicalEvent(ctx, wrap, tags, "wss://relay.one", wrap.FirstSeenAt); err != nil {
		t.Fatalf("insert gift wrap: %v", err)
	}

	if err := handlers.MarkAuthorAnalyticsDirty(ctx, wrap.ID); err != nil {
		t.Fatalf("MarkAuthorAnalyticsDirty: %v", err)
	}
	if err := handlers.MarkProfileStatsDirty(ctx, wrap.ID); err != nil {
		t.Fatalf("MarkProfileStatsDirty: %v", err)
	}
	if err := handlers.ApplyIncrementalAuthorStats(ctx, wrap.ID); err != nil {
		t.Fatalf("ApplyIncrementalAuthorStats: %v", err)
	}
	if err := handlers.ProjectAuthorRecentEvent(ctx, wrap.ID); err != nil {
		t.Fatalf("ProjectAuthorRecentEvent: %v", err)
	}

	assertCount := func(query, what string, want int) {
		t.Helper()
		var got int
		if err := pool.QueryRow(ctx, query).Scan(&got); err != nil {
			t.Fatalf("count %s: %v", what, err)
		}
		if got != want {
			t.Fatalf("%s: got %d want %d", what, got, want)
		}
	}
	assertCount(`SELECT COUNT(*) FROM pending_author_analytics_recomputes`, "author analytics queue", 0)
	assertCount(`SELECT COUNT(*) FROM pending_profile_stats_recomputes`, "profile stats queue", 0)
	assertCount(`SELECT COUNT(*) FROM author_recent_events WHERE author_pubkey = 'ephemeral'`, "author recent", 0)
	assertCount(`SELECT COUNT(*) FROM profile_public_stats WHERE pubkey = 'ephemeral'`, "profile stats", 0)

	if _, err := pool.Exec(ctx, `INSERT INTO pending_author_analytics_recomputes (pubkey) VALUES ('ephemeral')`); err != nil {
		t.Fatalf("seed author analytics queue: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO pending_profile_stats_recomputes (pubkey) VALUES ('ephemeral')`); err != nil {
		t.Fatalf("seed profile stats queue: %v", err)
	}
	processed, err := handlers.DrainPendingAuthorAnalyticsBatch(ctx, 10)
	if err != nil {
		t.Fatalf("drain author analytics: %v", err)
	}
	if processed != 1 {
		t.Fatalf("author analytics drain processed %d", processed)
	}
	processed, err = handlers.DrainPendingProfileStatsBatch(ctx, 10)
	if err != nil {
		t.Fatalf("drain profile stats: %v", err)
	}
	if processed != 1 {
		t.Fatalf("profile stats drain processed %d", processed)
	}
	assertCount(`SELECT COUNT(*) FROM pending_author_analytics_recomputes`, "author analytics queue after drain", 0)
	assertCount(`SELECT COUNT(*) FROM pending_profile_stats_recomputes`, "profile stats queue after drain", 0)
	assertCount(`SELECT COUNT(*) FROM author_engagement_stats WHERE pubkey = 'ephemeral'`, "engagement stats", 0)
}

func TestPurgeProjectionlessPending_KeepsRealAuthors(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

	note := newEventForTest("purge_note", "alice", 1000, 1, nil, "hello", baseTime)
	wrap := newEventForTest("purge_wrap", "ephemeral", 1001, 1059, [][]string{{"p", "alice"}}, "ciphertext", baseTime)
	if err := pgStore.InsertCanonicalEvent(ctx, note, nil, "wss://relay.one", note.FirstSeenAt); err != nil {
		t.Fatalf("insert note: %v", err)
	}
	if err := pgStore.InsertCanonicalEvent(ctx, wrap, extractTagsFromRaw(t, wrap.RawJSON), "wss://relay.one", wrap.FirstSeenAt); err != nil {
		t.Fatalf("insert wrap: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO reaction_events (
			event_id, target_event_id, reactor_pubkey, content, created_at, derivation_version, target_pubkey
		) VALUES ($1, $1, 'alice', '+', 1000, 1, 'only_target')
	`, note.ID); err != nil {
		t.Fatalf("insert reaction: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pending_author_analytics_recomputes (pubkey, marked_at)
		VALUES
			('alice', now() - interval '3 hours'),
			('ephemeral', now() - interval '2 hours'),
			('only_target', now() - interval '1 hour'),
			('claimed_wrap', now() - interval '4 hours')
	`); err != nil {
		t.Fatalf("seed author queue: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE pending_author_analytics_recomputes
		SET claimed_at = now()
		WHERE pubkey = 'claimed_wrap'
	`); err != nil {
		t.Fatalf("claim wrap: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pending_profile_stats_recomputes (pubkey)
		VALUES ('ephemeral'), ('alice')
	`); err != nil {
		t.Fatalf("seed profile queue: %v", err)
	}

	deleted, err := handlers.PurgeProjectionlessPending(ctx, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted %d, want 2 (author + profile ephemeral rows)", deleted)
	}
	var remaining []string
	rows, err := pool.Query(ctx, `SELECT pubkey FROM pending_author_analytics_recomputes ORDER BY pubkey`)
	if err != nil {
		t.Fatalf("list remaining: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			t.Fatalf("scan remaining: %v", err)
		}
		remaining = append(remaining, pubkey)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read remaining: %v", err)
	}
	want := []string{"alice", "claimed_wrap", "only_target"}
	if len(remaining) != len(want) {
		t.Fatalf("remaining author queue %v, want %v", remaining, want)
	}
	for i := range want {
		if remaining[i] != want[i] {
			t.Fatalf("remaining author queue %v, want %v", remaining, want)
		}
	}
	var profileLeft int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM pending_profile_stats_recomputes WHERE pubkey = 'alice'`).Scan(&profileLeft); err != nil {
		t.Fatalf("count profile queue: %v", err)
	}
	if profileLeft != 1 {
		t.Fatalf("alice profile queue row = %d, want 1", profileLeft)
	}
}

func TestAuthorAnalyticsClaim_SkipsRecentlyRefreshedPubkey(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		SweeperRefreshGap: 3 * time.Minute,
	})
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 8, 2, 13, 0, 0, 0, time.UTC)
	wrap := newEventForTest("fresh_gap_wrap", "stale_wrap", 1002, 1059, [][]string{{"p", "alice"}}, "ciphertext", baseTime)
	if err := pgStore.InsertCanonicalEvent(ctx, wrap, extractTagsFromRaw(t, wrap.RawJSON), "wss://relay.one", wrap.FirstSeenAt); err != nil {
		t.Fatalf("insert wrap: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO author_engagement_stats (pubkey, window_days) VALUES ('fresh_author', 7)
	`); err != nil {
		t.Fatalf("insert engagement stats: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pending_author_analytics_recomputes (pubkey, marked_at)
		VALUES ('fresh_author', now() - interval '1 hour'), ('stale_wrap', now() - interval '2 hours')
	`); err != nil {
		t.Fatalf("seed queue: %v", err)
	}

	processed, err := handlers.DrainPendingAuthorAnalyticsBatch(ctx, 10)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed %d, want 1", processed)
	}
	var left string
	if err := pool.QueryRow(ctx, `SELECT pubkey FROM pending_author_analytics_recomputes`).Scan(&left); err != nil {
		t.Fatalf("remaining pubkey: %v", err)
	}
	if left != "fresh_author" {
		t.Fatalf("remaining pubkey %q, want fresh_author", left)
	}
}

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
