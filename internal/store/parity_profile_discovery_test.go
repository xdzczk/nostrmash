package store

import (
	"context"
	"errors"
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

func TestGetRelatedProfiles_RankedAndBounded(t *testing.T) {
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
		newDiscoveryEvent("focal_note_1", "focal_author", now.Add(-2*time.Hour), 1, [][]string{{"t", "nostr"}}, "focal 1"),
		newDiscoveryEvent("focal_note_2", "focal_author", now.Add(-3*time.Hour), 1, [][]string{{"t", "dev"}}, "focal 2"),
		newDiscoveryEvent("multi_topic_note", "multi_author", now.Add(-90*time.Minute), 1, [][]string{{"t", "nostr"}, {"t", "dev"}}, "multi"),
		newDiscoveryEvent("topic_only_note", "topic_only_author", now.Add(-80*time.Minute), 1, [][]string{{"t", "nostr"}}, "topic only"),
		newDiscoveryEvent("reply_only_note", "reply_only_author", now.Add(-70*time.Minute), 1, [][]string{{"e", "focal_note_1", "", "reply"}}, "reply only"),
		newDiscoveryEvent("interaction_only_note", "interaction_only_author", now.Add(-65*time.Minute), 1, nil, "interaction only"),
		newDiscoveryEvent("quote_only_note", "quote_only_author", now.Add(-60*time.Minute), 1, nil, "quote only"),
		newDiscoveryEvent("focal_reply_to_topic_only", "focal_author", now.Add(-50*time.Minute), 1, [][]string{{"e", "topic_only_note", "", "reply"}}, "focal outbound reply"),
		newDiscoveryEvent("multi_reaction_1", "multi_author", now.Add(-45*time.Minute), 7, [][]string{{"e", "focal_note_1"}}, "+"),
		newDiscoveryEvent("multi_repost_1", "multi_author", now.Add(-40*time.Minute), 6, [][]string{{"e", "focal_note_1"}}, ""),
		newDiscoveryEvent("interaction_reaction_1", "interaction_only_author", now.Add(-35*time.Minute), 7, [][]string{{"e", "focal_note_1"}}, "+"),
		newDiscoveryEvent("quote_repost_1", "quote_only_author", now.Add(-30*time.Minute), 6, [][]string{{"e", "focal_note_1"}}, ""),
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

	related, err := pgStore.GetRelatedProfiles(ctx, "focal_author", 3)
	if err != nil {
		t.Fatalf("GetRelatedProfiles: %v", err)
	}
	if len(related) == 0 {
		t.Fatalf("expected related profiles, got none")
	}
	if len(related) > 3 {
		t.Fatalf("expected bounded related profiles size <= 3, got %d", len(related))
	}
	if related[0].Pubkey != "multi_author" {
		t.Fatalf("expected multi_author to rank first due to multiple heuristics, got %#v", related[0])
	}
	for _, row := range related {
		if row.Pubkey == "focal_author" {
			t.Fatalf("focal author should not be in related list")
		}
		if len(row.Reasons) == 0 {
			t.Fatalf("expected heuristic reasons, got %#v", row)
		}
	}
}

func TestGetRelatedProfiles_SparseAndMissing(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()
	sparseEvent := newDiscoveryEvent("sparse_note", "sparse_author", now.Add(-2*time.Hour), 1, nil, "sparse")
	tags := extractDiscoveryTagsForStoreTest(t, sparseEvent.RawJSON)
	if err := pgStore.InsertCanonicalEvent(ctx, sparseEvent, tags, "wss://relay.one", sparseEvent.FirstSeenAt); err != nil {
		t.Fatalf("insert sparse event: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, sparseEvent.ID); err != nil {
		t.Fatalf("derive sparse event: %v", err)
	}

	sparseRelated, err := pgStore.GetRelatedProfiles(ctx, "sparse_author", 10)
	if err != nil {
		t.Fatalf("GetRelatedProfiles sparse_author: %v", err)
	}
	if len(sparseRelated) != 0 {
		t.Fatalf("expected sparse author to have no related profiles, got %#v", sparseRelated)
	}

	_, err = pgStore.GetRelatedProfiles(ctx, "missing_author", 10)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing author, got %v", err)
	}
}
