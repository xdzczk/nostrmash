package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestProjectProfilePublicStats_TracksCountsAndFollowerChanges(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

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
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

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
