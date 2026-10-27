package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectProfilePublicStats_TracksCountsAndFollowerChanges(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC)

	target := newEventForTest("target_evt", "bob", 1000, 1, nil, "{}", baseTime)
	aliceNote := newEventForTest("alice_note", "alice", 1001, 1, nil, `{"content":"note"}`, baseTime.Add(1*time.Second))
	aliceReply := newEventForTest(
		"alice_reply",
		"alice",
		1002,
		1,
		[][]string{{"e", "target_evt", "", "reply"}},
		`{"content":"reply"}`,
		baseTime.Add(2*time.Second),
	)
	bobFollowsAlice := newEventForTest(
		"bob_contacts_1",
		"bob",
		1003,
		3,
		[][]string{{"p", "alice"}},
		"",
		baseTime.Add(3*time.Second),
	)
	carolFollowsAliceAndDave := newEventForTest(
		"carol_contacts_1",
		"carol",
		1004,
		3,
		[][]string{{"p", "alice"}, {"p", "dave"}},
		"",
		baseTime.Add(4*time.Second),
	)
	carolDropsAlice := newEventForTest(
		"carol_contacts_2",
		"carol",
		1005,
		3,
		[][]string{{"p", "dave"}},
		"",
		baseTime.Add(5*time.Second),
	)

	events := []model.Event{target, aliceNote, aliceReply, bobFollowsAlice, carolFollowsAliceAndDave, carolDropsAlice}
	for _, event := range events {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)

	aliceStats := mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if aliceStats.FollowerCount != 1 || aliceStats.FollowingCount != 0 {
		t.Fatalf("unexpected alice follow stats: %#v", aliceStats)
	}
	if aliceStats.NoteCount != 1 || aliceStats.ReplyCount != 1 {
		t.Fatalf("unexpected alice note/reply stats: %#v", aliceStats)
	}
	if aliceStats.RecentActivityAt == nil || *aliceStats.RecentActivityAt != 1002 {
		t.Fatalf("unexpected alice recent activity: %#v", aliceStats.RecentActivityAt)
	}

	bobStats := mustReadProfilePublicStats(t, ctx, pgStore, "bob")
	if bobStats.FollowingCount != 1 {
		t.Fatalf("unexpected bob following count: %#v", bobStats)
	}
}

func TestProjectionRebuildScopes_ProfilePublicStatsFull(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 8, 11, 0, 0, 0, time.UTC)

	aliceNote := newEventForTest("rebuild_alice_note", "alice", 2001, 1, nil, `{"content":"note"}`, baseTime)
	bobFollowsAlice := newEventForTest("rebuild_bob_contacts", "bob", 2002, 3, [][]string{{"p", "alice"}}, "", baseTime.Add(1*time.Second))
	for _, event := range []model.Event{aliceNote, bobFollowsAlice} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)

	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationProfilePublicStats,
		TargetVersion:  2,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger full rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("execute full rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, run.ID)

	var version int
	if err := pool.QueryRow(ctx, `
		SELECT derivation_version
		FROM profile_public_stats
		WHERE pubkey = 'alice'
	`).Scan(&version); err != nil {
		t.Fatalf("query alice profile_public_stats version: %v", err)
	}
	if version != 2 {
		t.Fatalf("unexpected profile_public_stats derivation_version: got=%d want=2", version)
	}
}

func TestProjectProfilePublicStats_ContentCounters(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	events := []model.Event{
		newEventForTest("alice_plain", "alice", 50, 1, nil, "hello", baseTime),
		newEventForTest("alice_image", "alice", 60, 1, nil, "photo https://cdn.example/pic.png", baseTime),
		newEventForTest("alice_imeta", "alice", 65, 1, [][]string{{"imeta", "url https://cdn.example/file"}}, "look", baseTime),
		newEventForTest("alice_picture", "alice", 70, 20, nil, "", baseTime),
		newEventForTest("alice_article", "alice", 80, 30023, [][]string{{"d", "one"}}, "first", baseTime),
		newEventForTest("alice_article_edit", "alice", 90, 30023, [][]string{{"d", "one"}}, "edited", baseTime),
		newEventForTest("alice_article_two", "alice", 100, 30023, [][]string{{"d", "two"}}, "second", baseTime),
		newEventForTest("alice_zap", "alice", 110, 9735, [][]string{{"p", "bob"}, {"amount", "21000"}}, "", baseTime),
	}
	for _, event := range events {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive %s: %v", event.ID, err)
		}
	}

	alice := mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if alice.NoteCount != 3 || alice.ReplyCount != 0 {
		t.Fatalf("notes: %#v", alice)
	}
	if alice.MediaCount != 3 {
		t.Fatalf("media = %d, want 3", alice.MediaCount)
	}
	if alice.LongFormNoteCount != 2 {
		t.Fatalf("articles = %d, want 2", alice.LongFormNoteCount)
	}
	if alice.ContentZapCount != 1 || alice.ZapReceivedCount != 0 || alice.SatsZapped != 0 {
		t.Fatalf("alice zaps: received=%d sats=%d sent=%d", alice.ZapReceivedCount, alice.SatsZapped, alice.ContentZapCount)
	}
	if alice.TimeJoined == nil || *alice.TimeJoined != 50 {
		t.Fatalf("time_joined = %v, want 50", alice.TimeJoined)
	}

	bob := mustReadProfilePublicStats(t, ctx, pgStore, "bob")
	if bob.ZapReceivedCount != 1 || bob.SatsZapped != 21 || bob.ContentZapCount != 0 {
		t.Fatalf("bob zaps: received=%d sats=%d sent=%d", bob.ZapReceivedCount, bob.SatsZapped, bob.ContentZapCount)
	}

	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationProfilePublicStats,
		TargetVersion:  2,
		Scope:          derivation.ProjectionRebuildScope{Type: derivation.RebuildScopeFull},
	})
	if err != nil {
		t.Fatalf("trigger rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	rebuilt := mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if rebuilt.NoteCount != alice.NoteCount || rebuilt.MediaCount != alice.MediaCount || rebuilt.LongFormNoteCount != alice.LongFormNoteCount || rebuilt.ContentZapCount != alice.ContentZapCount || rebuilt.TimeJoined == nil || *rebuilt.TimeJoined != 50 {
		t.Fatalf("rebuild drifted: before=%#v after=%#v", alice, rebuilt)
	}

	reverseEventTx(t, ctx, handlers, pool, "alice_zap")
	alice = mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	bob = mustReadProfilePublicStats(t, ctx, pgStore, "bob")
	if alice.ContentZapCount != 0 || bob.ZapReceivedCount != 0 || bob.SatsZapped != 0 {
		t.Fatalf("zap reversal: alice sent=%d bob received=%d sats=%d", alice.ContentZapCount, bob.ZapReceivedCount, bob.SatsZapped)
	}

	reverseEventTx(t, ctx, handlers, pool, "alice_article")
	alice = mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if alice.LongFormNoteCount != 2 {
		t.Fatalf("replaced article reversed the count to %d", alice.LongFormNoteCount)
	}
	reverseEventTx(t, ctx, handlers, pool, "alice_article_two")
	reverseEventTx(t, ctx, handlers, pool, "alice_article_two")
	alice = mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if alice.LongFormNoteCount != 1 {
		t.Fatalf("current article count = %d, want 1 after one reversal", alice.LongFormNoteCount)
	}
}

func mustReadProfilePublicStats(
	t *testing.T,
	ctx context.Context,
	pgStore *store.PostgresStore,
	pubkey string,
) store.ProfilePublicStatsProjection {
	t.Helper()
	row, err := pgStore.GetProfilePublicStatsByPubkey(ctx, pubkey)
	if err != nil {
		t.Fatalf("get profile public stats %s: %v", pubkey, err)
	}
	return row
}
