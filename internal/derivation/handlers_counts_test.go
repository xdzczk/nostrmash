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

func TestProjectCounts_ReplyReactionRepost(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 18, 0, 0, 0, time.UTC)

	target := newEventForTest("target_evt", "author_target", 1000, 1, nil, "{}", baseTime)
	reply := newEventForTest(
		"reply_evt",
		"author_reply",
		1001,
		1,
		[][]string{{"e", "target_evt", "", "reply"}},
		`{"content":"reply"}`,
		baseTime.Add(1*time.Second),
	)
	reactionA := newEventForTest(
		"react_a",
		"author_react_a",
		1002,
		7,
		[][]string{{"e", "target_evt"}},
		`{"content":"+"}`,
		baseTime.Add(2*time.Second),
	)
	reactionWithDuplicateTags := newEventForTest(
		"react_b",
		"author_react_b",
		1003,
		7,
		[][]string{{"e", "target_evt"}, {"e", "target_evt"}},
		`{"content":"++"}`,
		baseTime.Add(3*time.Second),
	)
	repost := newEventForTest(
		"repost_evt",
		"author_repost",
		1004,
		6,
		[][]string{{"e", "target_evt"}},
		`{"content":"repost"}`,
		baseTime.Add(4*time.Second),
	)

	events := []model.Event{target, reply, reactionA, reactionWithDuplicateTags, repost}
	for _, event := range events {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}

	if err := handlers.ProjectReplyCounts(ctx, reply.ID); err != nil {
		t.Fatalf("project reply counts: %v", err)
	}
	if err := handlers.ProjectReactionCounts(ctx, reactionA.ID); err != nil {
		t.Fatalf("project reaction counts a: %v", err)
	}
	if err := handlers.ProjectReactionCounts(ctx, reactionWithDuplicateTags.ID); err != nil {
		t.Fatalf("project reaction counts b: %v", err)
	}
	if err := handlers.ProjectRepostCounts(ctx, repost.ID); err != nil {
		t.Fatalf("project repost counts: %v", err)
	}
	// Idempotency: rerunning should preserve stable counts.
	if err := handlers.ProjectReactionCounts(ctx, reactionWithDuplicateTags.ID); err != nil {
		t.Fatalf("project reaction counts b again: %v", err)
	}

	counts, err := pgStore.GetEventCounts(ctx, "target_evt")
	if err != nil {
		t.Fatalf("get event counts: %v", err)
	}
	if counts.ReplyCount != 1 {
		t.Fatalf("unexpected reply_count: got=%d want=1", counts.ReplyCount)
	}
	if counts.ReactionCount != 2 {
		t.Fatalf("unexpected reaction_count: got=%d want=2", counts.ReactionCount)
	}
	if counts.RepostCount != 1 {
		t.Fatalf("unexpected repost_count: got=%d want=1", counts.RepostCount)
	}
}
