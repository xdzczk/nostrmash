package store

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetTrendingAndRisingProfiles_WindowsAndOrdering(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newDiscoveryEvent("small_note", "small_author", now.Add(-2*time.Hour), 1, nil, "small note"),
		newDiscoveryEvent("big_note", "big_author", now.Add(-3*time.Hour), 1, nil, "big note"),
		newDiscoveryEvent("older_note", "older_author", now.Add(-72*time.Hour), 1, nil, "older note"),

		newDiscoveryEvent("small_reply_1", "small_replier_1", now.Add(-90*time.Minute), 1, [][]string{{"e", "small_note", "", "reply"}}, "reply"),
		newDiscoveryEvent("small_reply_2", "small_replier_2", now.Add(-80*time.Minute), 1, [][]string{{"e", "small_note", "", "reply"}}, "reply"),
		newDiscoveryEvent("small_reaction_1", "small_reactor_1", now.Add(-70*time.Minute), 7, [][]string{{"e", "small_note"}}, "+"),
		newDiscoveryEvent("small_reaction_2", "small_reactor_2", now.Add(-65*time.Minute), 7, [][]string{{"e", "small_note"}}, "+"),
		newDiscoveryEvent("small_reaction_3", "small_reactor_3", now.Add(-60*time.Minute), 7, [][]string{{"e", "small_note"}}, "+"),
		newDiscoveryEvent("small_reaction_4", "small_reactor_4", now.Add(-55*time.Minute), 7, [][]string{{"e", "small_note"}}, "+"),

		newDiscoveryEvent("big_reply_1", "big_replier_1", now.Add(-50*time.Minute), 1, [][]string{{"e", "big_note", "", "reply"}}, "reply"),
		newDiscoveryEvent("big_reply_2", "big_replier_2", now.Add(-45*time.Minute), 1, [][]string{{"e", "big_note", "", "reply"}}, "reply"),
		newDiscoveryEvent("big_reaction_1", "big_reactor_1", now.Add(-40*time.Minute), 7, [][]string{{"e", "big_note"}}, "+"),
		newDiscoveryEvent("big_reaction_2", "big_reactor_2", now.Add(-35*time.Minute), 7, [][]string{{"e", "big_note"}}, "+"),
		newDiscoveryEvent("big_reaction_3", "big_reactor_3", now.Add(-30*time.Minute), 7, [][]string{{"e", "big_note"}}, "+"),
		newDiscoveryEvent("big_reaction_4", "big_reactor_4", now.Add(-25*time.Minute), 7, [][]string{{"e", "big_note"}}, "+"),
		newDiscoveryEvent("big_reaction_5", "big_reactor_5", now.Add(-20*time.Minute), 7, [][]string{{"e", "big_note"}}, "+"),

		newDiscoveryEvent("older_reply_1", "older_replier_1", now.Add(-50*time.Hour), 1, [][]string{{"e", "older_note", "", "reply"}}, "reply"),
		newDiscoveryEvent("older_reaction_1", "older_reactor_1", now.Add(-49*time.Hour), 7, [][]string{{"e", "older_note"}}, "+"),
	}

	for i := 0; i < 25; i++ {
		id := "follow_big_" + string(rune('a'+i))
		follower := "follower_big_" + string(rune('a'+i))
		events = append(events, newDiscoveryEvent(
			id,
			follower,
			now.Add(-6*time.Hour),
			3,
			[][]string{{"p", "big_author"}},
			"contacts",
		))
	}

	for _, event := range events {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	trending24h, err := pgStore.GetTrendingProfiles(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingProfiles 24h: %v", err)
	}
	if len(trending24h) != 2 {
		t.Fatalf("unexpected 24h trending profile count: got=%d want=2", len(trending24h))
	}

	rising24h, err := pgStore.GetRisingProfiles(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetRisingProfiles 24h: %v", err)
	}
	if len(rising24h) != 2 {
		t.Fatalf("unexpected 24h rising profile count: got=%d want=2", len(rising24h))
	}
	if rising24h[0].Pubkey != "small_author" {
		t.Fatalf("expected small_author to rank above big_author in rising 24h, got=%#v", rising24h[0])
	}

	trending7d, err := pgStore.GetTrendingProfiles(ctx, 7*24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingProfiles 7d: %v", err)
	}
	if len(trending7d) != 3 {
		t.Fatalf("unexpected 7d trending profile count: got=%d want=3", len(trending7d))
	}
}
