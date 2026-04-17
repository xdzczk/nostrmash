package derivation_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestProjectProfileDiscoveryStats_TracksScoresAndRisingOrder(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newEventForTest("small_note_evt", "small_author", now.Add(-2*time.Hour).Unix(), 1, nil, "small", now.Add(-2*time.Hour)),
		newEventForTest("big_note_evt", "big_author", now.Add(-2*time.Hour).Unix(), 1, nil, "big", now.Add(-2*time.Hour)),
		newEventForTest("small_reply_evt", "small_reply", now.Add(-70*time.Minute).Unix(), 1, [][]string{{"e", "small_note_evt", "", "reply"}}, "reply", now.Add(-70*time.Minute)),
		newEventForTest("small_reaction_evt", "small_reactor", now.Add(-65*time.Minute).Unix(), 7, [][]string{{"e", "small_note_evt"}}, "+", now.Add(-65*time.Minute)),
		newEventForTest("big_reply_evt", "big_reply", now.Add(-70*time.Minute).Unix(), 1, [][]string{{"e", "big_note_evt", "", "reply"}}, "reply", now.Add(-70*time.Minute)),
		newEventForTest("big_reaction_evt", "big_reactor", now.Add(-65*time.Minute).Unix(), 7, [][]string{{"e", "big_note_evt"}}, "+", now.Add(-65*time.Minute)),
	}
	for i := 0; i < 20; i++ {
		events = append(events, newEventForTest(
			fmt.Sprintf("big_follower_evt_%d", i),
			fmt.Sprintf("big_follower_%d", i),
			now.Add(-4*time.Hour).Unix(),
			3,
			[][]string{{"p", "big_author"}},
			"contacts",
			now.Add(-4*time.Hour),
		))
	}

	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)

	var smallScore24h float64
	var smallRising24h float64
	if err := pool.QueryRow(ctx, `
		SELECT score_24h, rising_score_24h
		FROM profile_discovery_stats
		WHERE pubkey = 'small_author'
	`).Scan(&smallScore24h, &smallRising24h); err != nil {
		t.Fatalf("query small_author profile discovery stats: %v", err)
	}
	if smallScore24h <= 0 || smallRising24h <= 0 {
		t.Fatalf("expected small author to have positive scores, got score=%f rising=%f", smallScore24h, smallRising24h)
	}

	var topRisingPubkey string
	if err := pool.QueryRow(ctx, `
		SELECT pubkey
		FROM profile_discovery_stats
		WHERE rising_score_24h > 0
		ORDER BY rising_score_24h DESC, pubkey ASC
		LIMIT 1
	`).Scan(&topRisingPubkey); err != nil {
		t.Fatalf("query top rising pubkey: %v", err)
	}
	if topRisingPubkey != "big_author" {
		t.Fatalf("expected big_author to lead rising ranking via follower growth, got %s", topRisingPubkey)
	}
}

func TestProjectionRebuildScopes_ProfileDiscoveryStatsFull(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	note := newEventForTest("profile_rebuild_note", "profile_rebuild_author", time.Now().UTC().Add(-1*time.Hour).Unix(), 1, nil, "rebuild profile stats", time.Now().UTC().Add(-1*time.Hour))
	if err := pgStore.InsertCanonicalEvent(ctx, note, extractTagsFromRaw(t, note.RawJSON), "wss://relay.one", note.FirstSeenAt); err != nil {
		t.Fatalf("insert note: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, note.ID); err != nil {
		t.Fatalf("derive note bundle: %v", err)
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)

	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationProfileDiscoveryStats,
		TargetVersion:  2,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger profile discovery rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("execute profile discovery rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationProfileDiscoveryStats, 2, 2)

	var version int
	if err := pool.QueryRow(ctx, `
		SELECT derivation_version
		FROM profile_discovery_stats
		WHERE pubkey = $1
	`, "profile_rebuild_author").Scan(&version); err != nil {
		t.Fatalf("query profile discovery derivation version: %v", err)
	}
	if version != 2 {
		t.Fatalf("unexpected profile discovery derivation version: got=%d want=2", version)
	}
}
