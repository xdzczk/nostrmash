package derivation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
