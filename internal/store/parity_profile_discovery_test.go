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
		newDiscoveryEvent("meta_small", "small_author", now.Add(-5*time.Hour), 0, nil, `{"name":"small"}`),
		newDiscoveryEvent("meta_big", "big_author", now.Add(-5*time.Hour), 0, nil, `{"name":"big"}`),
		newDiscoveryEvent("meta_older", "older_author", now.Add(-80*time.Hour), 0, nil, `{"name":"older"}`),
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
	if rising24h[0].Pubkey != "big_author" {
		t.Fatalf("expected big_author to rank above small_author in rising 24h due to follower growth, got=%#v", rising24h[0])
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
		newDiscoveryEvent("meta_focal", "focal_author", now.Add(-4*time.Hour), 0, nil, `{"name":"focal"}`),
		newDiscoveryEvent("meta_multi", "multi_author", now.Add(-4*time.Hour), 0, nil, `{"name":"multi"}`),
		newDiscoveryEvent("meta_topic_only", "topic_only_author", now.Add(-4*time.Hour), 0, nil, `{"name":"topic"}`),
		newDiscoveryEvent("meta_reply_only", "reply_only_author", now.Add(-4*time.Hour), 0, nil, `{"name":"reply"}`),
		newDiscoveryEvent("meta_interaction_only", "interaction_only_author", now.Add(-4*time.Hour), 0, nil, `{"name":"interaction"}`),
		newDiscoveryEvent("meta_quote_only", "quote_only_author", now.Add(-4*time.Hour), 0, nil, `{"name":"quote"}`),
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

func TestGetTrendingProfiles_ExcludesProfilesWithoutLocalMetadata(t *testing.T) {
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
		newDiscoveryEvent("meta_resolved", "resolved_author", now.Add(-4*time.Hour), 0, nil, `{"name":"resolved"}`),
		newDiscoveryEvent("resolved_note", "resolved_author", now.Add(-2*time.Hour), 1, nil, "resolved note"),
		newDiscoveryEvent("resolved_reaction_1", "resolved_reactor_1", now.Add(-90*time.Minute), 7, [][]string{{"e", "resolved_note"}}, "+"),
		newDiscoveryEvent("resolved_reaction_2", "resolved_reactor_2", now.Add(-80*time.Minute), 7, [][]string{{"e", "resolved_note"}}, "+"),
		newDiscoveryEvent("unresolved_note", "unresolved_author", now.Add(-70*time.Minute), 1, nil, "unresolved note"),
		newDiscoveryEvent("unresolved_reaction_1", "unresolved_reactor_1", now.Add(-60*time.Minute), 7, [][]string{{"e", "unresolved_note"}}, "+"),
		newDiscoveryEvent("unresolved_reaction_2", "unresolved_reactor_2", now.Add(-50*time.Minute), 7, [][]string{{"e", "unresolved_note"}}, "+"),
		newDiscoveryEvent("unresolved_reaction_3", "unresolved_reactor_3", now.Add(-40*time.Minute), 7, [][]string{{"e", "unresolved_note"}}, "+"),
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

	profiles, err := pgStore.GetTrendingProfiles(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingProfiles: %v", err)
	}
	if len(profiles) != 1 {
		t.Fatalf("expected only resolved profiles to be returned, got %#v", profiles)
	}
	if profiles[0].Pubkey != "resolved_author" {
		t.Fatalf("expected resolved_author only, got %#v", profiles)
	}
}

func TestTrendingAndRisingProfiles_PenalizeHighVolumeLowEngagement(t *testing.T) {
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
		newDiscoveryEvent("meta_spammy", "spammy_author", now.Add(-6*time.Hour), 0, nil, `{"name":"spammy"}`),
		newDiscoveryEvent("meta_organic", "organic_author", now.Add(-6*time.Hour), 0, nil, `{"name":"organic"}`),
	}
	for i := 0; i < 18; i++ {
		events = append(events, newDiscoveryEvent(
			"spammy_note_"+string(rune('a'+i)),
			"spammy_author",
			now.Add(time.Duration(-180+i)*time.Minute),
			1,
			nil,
			"spam note",
		))
	}
	for i := 0; i < 4; i++ {
		events = append(events, newDiscoveryEvent(
			"organic_note_"+string(rune('a'+i)),
			"organic_author",
			now.Add(time.Duration(-200+i)*time.Minute),
			1,
			nil,
			"organic note",
		))
	}
	events = append(events,
		newDiscoveryEvent("spammy_reaction_1", "spammy_reactor", now.Add(-20*time.Minute), 7, [][]string{{"e", "spammy_note_a"}}, "+"),
		newDiscoveryEvent("organic_reaction_1", "org_reactor_1", now.Add(-40*time.Minute), 7, [][]string{{"e", "organic_note_a"}}, "+"),
		newDiscoveryEvent("organic_reaction_2", "org_reactor_2", now.Add(-39*time.Minute), 7, [][]string{{"e", "organic_note_a"}}, "+"),
		newDiscoveryEvent("organic_reaction_3", "org_reactor_3", now.Add(-38*time.Minute), 7, [][]string{{"e", "organic_note_b"}}, "+"),
		newDiscoveryEvent("organic_reaction_4", "org_reactor_4", now.Add(-37*time.Minute), 7, [][]string{{"e", "organic_note_b"}}, "+"),
		newDiscoveryEvent("organic_reaction_5", "org_reactor_5", now.Add(-36*time.Minute), 7, [][]string{{"e", "organic_note_c"}}, "+"),
		newDiscoveryEvent("organic_reaction_6", "org_reactor_6", now.Add(-35*time.Minute), 7, [][]string{{"e", "organic_note_c"}}, "+"),
		newDiscoveryEvent("organic_reply_1", "org_replier_1", now.Add(-34*time.Minute), 1, [][]string{{"e", "organic_note_d", "", "reply"}}, "reply"),
		newDiscoveryEvent("organic_reply_2", "org_replier_2", now.Add(-33*time.Minute), 1, [][]string{{"e", "organic_note_d", "", "reply"}}, "reply"),
	)

	for _, event := range events {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	trending, err := pgStore.GetTrendingProfiles(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingProfiles: %v", err)
	}
	if len(trending) < 2 {
		t.Fatalf("expected at least 2 profiles, got %#v", trending)
	}
	if trending[0].Pubkey != "organic_author" {
		t.Fatalf("expected organic_author to outrank spammy_author in trending, got %#v", trending[0])
	}

	rising, err := pgStore.GetRisingProfiles(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetRisingProfiles: %v", err)
	}
	if len(rising) < 2 {
		t.Fatalf("expected at least 2 profiles, got %#v", rising)
	}
	if rising[0].Pubkey != "organic_author" {
		t.Fatalf("expected organic_author to outrank spammy_author in rising, got %#v", rising[0])
	}
}

func TestTrendingVsRisingProfiles_EngagementVsFollowerGrowth(t *testing.T) {
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
		newDiscoveryEvent("meta_engaged", "engaged_author", now.Add(-5*time.Hour), 0, nil, `{"name":"engaged"}`),
		newDiscoveryEvent("meta_growth", "growth_author", now.Add(-5*time.Hour), 0, nil, `{"name":"growth"}`),
		newDiscoveryEvent("engaged_note", "engaged_author", now.Add(-2*time.Hour), 1, nil, "engaged note"),
		newDiscoveryEvent("growth_note", "growth_author", now.Add(-2*time.Hour), 1, nil, "growth note"),
	}
	for i := 0; i < 8; i++ {
		events = append(events, newDiscoveryEvent(
			"engaged_reaction_"+string(rune('a'+i)),
			"engaged_reactor_"+string(rune('a'+i)),
			now.Add(time.Duration(-90+i)*time.Minute),
			7,
			[][]string{{"e", "engaged_note"}},
			"+",
		))
	}
	for i := 0; i < 10; i++ {
		events = append(events, newDiscoveryEvent(
			"growth_follow_"+string(rune('a'+i)),
			"growth_follower_"+string(rune('a'+i)),
			now.Add(time.Duration(-200+i)*time.Minute),
			3,
			[][]string{{"p", "growth_author"}},
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

	trending, err := pgStore.GetTrendingProfiles(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingProfiles: %v", err)
	}
	if len(trending) < 2 {
		t.Fatalf("expected at least 2 profiles, got %#v", trending)
	}
	if trending[0].Pubkey != "engaged_author" {
		t.Fatalf("expected engagement-led profile first in trending, got %#v", trending[0])
	}

	rising, err := pgStore.GetRisingProfiles(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetRisingProfiles: %v", err)
	}
	if len(rising) < 2 {
		t.Fatalf("expected at least 2 profiles, got %#v", rising)
	}
	if rising[0].Pubkey != "growth_author" {
		t.Fatalf("expected follower-growth profile first in rising, got %#v", rising[0])
	}
}
