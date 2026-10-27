package derivation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectContactListsLatest_DerivesFollowerEdgesFromLatestList(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 5, 10, 0, 0, 0, time.UTC)

	firstTags := [][]string{
		{"p", "bob"},
		{"p", "carol"},
	}
	first := newEventForTest(
		"contact_evt_1",
		"alice",
		1000,
		3,
		firstTags,
		`{"content":"contacts1"}`,
		baseTime,
	)
	secondTags := [][]string{
		{"p", "carol"},
	}
	second := newEventForTest(
		"contact_evt_2",
		"alice",
		1001,
		3,
		secondTags,
		`{"content":"contacts2"}`,
		baseTime.Add(1*time.Second),
	)
	insertFixtures := []struct {
		event model.Event
		tags  [][]string
	}{
		{event: first, tags: firstTags},
		{event: second, tags: secondTags},
	}
	for _, fixture := range insertFixtures {
		if err := pgStore.InsertCanonicalEvent(ctx, fixture.event, fixture.tags, "wss://relay.one", fixture.event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", fixture.event.ID, err)
		}
	}

	if err := handlers.ProjectContactListsLatest(ctx, first.ID); err != nil {
		t.Fatalf("project first contact list: %v", err)
	}
	bobFollowers, err := pgStore.GetFollowersByPubkey(ctx, "bob", 20)
	if err != nil {
		t.Fatalf("get bob followers after first projection: %v", err)
	}
	if len(bobFollowers) != 1 {
		t.Fatalf("expected bob to have one follower after first projection, got %d", len(bobFollowers))
	}

	if err := handlers.ProjectContactListsLatest(ctx, second.ID); err != nil {
		t.Fatalf("project second contact list: %v", err)
	}
	bobFollowers, err = pgStore.GetFollowersByPubkey(ctx, "bob", 20)
	if err != nil {
		t.Fatalf("get bob followers after replacement: %v", err)
	}
	if len(bobFollowers) != 0 {
		t.Fatalf("expected bob followers to be removed after replacement, got %d", len(bobFollowers))
	}

	carolFollowers, err := pgStore.GetFollowersByPubkey(ctx, "carol", 20)
	if err != nil {
		t.Fatalf("get carol followers after replacement: %v", err)
	}
	if len(carolFollowers) != 1 {
		t.Fatalf("expected carol to have one follower, got %d", len(carolFollowers))
	}
	var followerRow struct {
		FollowerPubkey string `json:"follower_pubkey"`
		SourceEventID  string `json:"source_event_id"`
	}
	if err := json.Unmarshal(carolFollowers[0], &followerRow); err != nil {
		t.Fatalf("decode follower row: %v", err)
	}
	if followerRow.FollowerPubkey != "alice" || followerRow.SourceEventID != second.ID {
		t.Fatalf("unexpected follower row: %+v", followerRow)
	}
}

func TestProjectContactListsLatest_HistoricalImportSkipsFollowerGains(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	oldCreatedAt := now.Add(-200 * time.Hour).Unix()

	oldList := newEventForTest("hist_old", "alice", oldCreatedAt, 3, [][]string{{"p", "bob"}, {"p", "carol"}}, "", now)
	editedList := newEventForTest("hist_new", "alice", now.Unix(), 3, [][]string{{"p", "bob"}, {"p", "eve"}}, "", now)
	recentList := newEventForTest("recent_list", "dave", now.Unix(), 3, [][]string{{"p", "frank"}}, "", now)
	for _, event := range []model.Event{oldList, editedList, recentList} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
	}

	if err := handlers.ProjectContactListsLatest(ctx, oldList.ID); err != nil {
		t.Fatalf("project old list: %v", err)
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)
	if gains := countFollowerGains(t, ctx, pool, "alice"); gains != 0 {
		t.Fatalf("historical import gains = %d, want 0", gains)
	}
	if followers := countFollowerEdges(t, ctx, pool, "bob"); followers != 1 {
		t.Fatalf("bob followers after historical import = %d, want 1", followers)
	}
	if count := profileFollowerCount(t, ctx, pool, "bob"); count != 1 {
		t.Fatalf("bob follower_count = %d, want 1", count)
	}

	if err := handlers.ProjectContactListsLatest(ctx, editedList.ID); err != nil {
		t.Fatalf("project edited list: %v", err)
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)
	if gains := countFollowerGains(t, ctx, pool, "alice"); gains != 1 {
		t.Fatalf("edited list gains = %d, want 1", gains)
	}
	var gained string
	if err := pool.QueryRow(ctx, `SELECT followed_pubkey FROM follower_gain_events WHERE follower_pubkey = 'alice'`).Scan(&gained); err != nil {
		t.Fatalf("read gain: %v", err)
	}
	if gained != "eve" {
		t.Fatalf("gained pubkey = %q, want eve", gained)
	}
	if followers := countFollowerEdges(t, ctx, pool, "carol"); followers != 0 {
		t.Fatalf("carol followers after edit = %d, want 0", followers)
	}

	if err := handlers.ProjectContactListsLatest(ctx, recentList.ID); err != nil {
		t.Fatalf("project recent list: %v", err)
	}
	if gains := countFollowerGains(t, ctx, pool, "dave"); gains != 1 {
		t.Fatalf("recent list gains = %d, want 1", gains)
	}
}

func countFollowerGains(t *testing.T, ctx context.Context, pool *pgxpool.Pool, follower string) int {
	t.Helper()
	return queryInt(t, ctx, pool, `SELECT COUNT(*) FROM follower_gain_events WHERE follower_pubkey = $1`, follower)
}

func countFollowerEdges(t *testing.T, ctx context.Context, pool *pgxpool.Pool, followed string) int {
	t.Helper()
	return queryInt(t, ctx, pool, `SELECT COUNT(*) FROM follower_edges WHERE followed_pubkey = $1`, followed)
}

func profileFollowerCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) int {
	t.Helper()
	return queryInt(t, ctx, pool, `SELECT follower_count FROM profile_public_stats WHERE pubkey = $1`, pubkey)
}

func queryInt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, arg string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, arg).Scan(&n); err != nil {
		t.Fatalf("query %s: %v", sql, err)
	}
	return n
}
