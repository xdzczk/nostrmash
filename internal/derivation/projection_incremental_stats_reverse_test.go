package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

// TestReverseIncrementalAuthorStatsTx_UndoesAllProjections is the retention
// decrement-path regression test: it applies incremental deltas for a note
// (with a hashtag, media, and an incoming reply) exactly like
// ApplyIncrementalAuthorStats does, then reverses them via
// ReverseIncrementalAuthorStatsTx and asserts every touched counter is back
// to its pre-event value. This guards the fix for the "retention purges
// don't decrement incremental counters" gap.
func TestReverseIncrementalAuthorStatsTx_UndoesAllProjections(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		IncrementalProfilePublicStats:  boolPtr(true),
		IncrementalAuthorActivityDaily: boolPtr(true),
		IncrementalWindowedRollups:     boolPtr(true),
	})
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	aliceNote := newEventForTest(
		"rev_alice_note",
		"alice",
		now.Add(-2*time.Hour).Unix(),
		1,
		[][]string{{"t", "nostr"}, {"image", "https://cdn/img.png"}},
		`{"content":"image note"}`,
		now,
	)
	bobReply := newEventForTest(
		"rev_bob_reply",
		"bob",
		now.Add(-1*time.Hour).Unix(),
		1,
		[][]string{{"e", "rev_alice_note", "", "reply"}},
		`{"content":"nice"}`,
		now,
	)

	for _, event := range []model.Event{aliceNote, bobReply} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	// Sanity: forward deltas landed before we try to reverse them.
	aliceStats := mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if aliceStats.NoteCount != 1 || aliceStats.MediaCount != 1 {
		t.Fatalf("expected alice note_count=1 media_count=1 before reversal, got %#v", aliceStats)
	}
	if aliceStats.TimeJoined == nil || *aliceStats.TimeJoined != aliceNote.CreatedAt {
		t.Fatalf("expected alice time_joined=%d before reversal, got %#v", aliceNote.CreatedAt, aliceStats.TimeJoined)
	}
	bobStats := mustReadProfilePublicStats(t, ctx, pgStore, "bob")
	if bobStats.ReplyCount != 1 {
		t.Fatalf("expected bob reply_count=1 before reversal, got %#v", bobStats)
	}
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(post_count),0) FROM author_activity_daily WHERE pubkey='alice'`, 1)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(engagement_received),0) FROM author_activity_daily WHERE pubkey='alice'`, 1)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(usage_count),0) FROM author_hashtag_daily WHERE pubkey='alice' AND hashtag='nostr'`, 1)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(total_posts),0) FROM author_media_daily WHERE pubkey='alice'`, 1)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(with_image_count),0) FROM author_media_daily WHERE pubkey='alice'`, 1)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(post_count),0) FROM author_hourly_activity WHERE pubkey='alice'`, 1)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(reply_received),0) FROM author_hourly_activity WHERE pubkey='alice'`, 1)

	// Reverse the reply first (as retention would, purging newest-independent
	// batches), then the original note — mirrors an out-of-order purge batch
	// touching both events.
	reverseEventTx(t, ctx, handlers, pool, bobReply.ID)
	reverseEventTx(t, ctx, handlers, pool, aliceNote.ID)

	aliceStats = mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if aliceStats.NoteCount != 0 || aliceStats.MediaCount != 0 || aliceStats.TimeJoined != nil {
		t.Fatalf("expected alice counters cleared after reversal, got %#v", aliceStats)
	}
	bobStats = mustReadProfilePublicStats(t, ctx, pgStore, "bob")
	if bobStats.ReplyCount != 0 {
		t.Fatalf("expected bob reply_count=0 after reversal, got %#v", bobStats)
	}
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(post_count),0) FROM author_activity_daily WHERE pubkey='alice'`, 0)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(engagement_received),0) FROM author_activity_daily WHERE pubkey='alice'`, 0)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(usage_count),0) FROM author_hashtag_daily WHERE pubkey='alice' AND hashtag='nostr'`, 0)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(total_posts),0) FROM author_media_daily WHERE pubkey='alice'`, 0)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(with_image_count),0) FROM author_media_daily WHERE pubkey='alice'`, 0)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(post_count),0) FROM author_hourly_activity WHERE pubkey='alice'`, 0)
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(reply_received),0) FROM author_hourly_activity WHERE pubkey='alice'`, 0)
	assertScalar(t, ctx, pool, `SELECT COUNT(*) FROM applied_stat_deltas WHERE event_id IN ('rev_alice_note','rev_bob_reply')`, 0)

	// Idempotency: reversing again (e.g. a retried purge batch after a crash
	// between reversal and delete) must be a safe no-op, not a double
	// decrement below zero.
	reverseEventTx(t, ctx, handlers, pool, aliceNote.ID)
	aliceStats = mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if aliceStats.NoteCount != 0 {
		t.Fatalf("expected alice note_count to remain 0 after repeated reversal, got %#v", aliceStats)
	}
	assertScalar(t, ctx, pool, `SELECT COALESCE(SUM(post_count),0) FROM author_activity_daily WHERE pubkey='alice'`, 0)
}

// reverseEventTx runs ReverseIncrementalAuthorStatsTx for eventID in its own
// committed transaction, mirroring how a retention purge wrapper would call
// it: reverse, then (in production) delete — here we skip the delete so the
// test can assert on the still-queryable event_hashtags/note_discovery_stats
// rows and exercise repeated-reversal idempotency without re-deriving.
func TestRefreshProfileTimeJoinedTx_AfterBatchDelete(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)
	older := newEventForTest("joined_old", "carol", 50, 1, nil, "old", baseTime)
	newer := newEventForTest("joined_new", "carol", 80, 1, nil, "new", baseTime)
	for _, event := range []model.Event{older, newer} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive %s: %v", event.ID, err)
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, id := range []string{older.ID, newer.ID} {
		if err := handlers.ReverseIncrementalAuthorStatsTx(ctx, tx, id); err != nil {
			t.Fatalf("reverse %s: %v", id, err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM events WHERE id = ANY($1)`, []string{older.ID, newer.ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := handlers.RefreshProfileTimeJoinedTx(ctx, tx, []string{"carol"}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	stats := mustReadProfilePublicStats(t, ctx, pgStore, "carol")
	if stats.NoteCount != 0 || stats.TimeJoined != nil {
		t.Fatalf("after deleting both events: %#v", stats)
	}
}

func reverseEventTx(t *testing.T, ctx context.Context, handlers *derivation.Handlers, pool *pgxpool.Pool, eventID string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin reversal tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := handlers.ReverseIncrementalAuthorStatsTx(ctx, tx, eventID); err != nil {
		t.Fatalf("reverse incremental stats for %s: %v", eventID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit reversal tx for %s: %v", eventID, err)
	}
}

func assertScalar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, want int64) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(ctx, query).Scan(&got); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("query %q: got %d want %d", query, got, want)
	}
}
