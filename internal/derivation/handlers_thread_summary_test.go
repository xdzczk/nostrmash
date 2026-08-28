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

func TestThreadSummaryProjection_RebuildCorrectness(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	root := newEventForTest("thread_summary_rebuild_root", "root_author", now.Add(-3*time.Hour).Unix(), 1, nil, "root", now.Add(-3*time.Hour))
	replyOne := newEventForTest(
		"thread_summary_rebuild_reply_one",
		"reply_author_one",
		now.Add(-2*time.Hour).Unix(),
		1,
		[][]string{{"e", root.ID, "", "reply"}, {"e", root.ID, "", "root"}},
		"reply one",
		now.Add(-2*time.Hour),
	)
	replyTwo := newEventForTest(
		"thread_summary_rebuild_reply_two",
		"reply_author_two",
		now.Add(-30*time.Minute).Unix(),
		1,
		[][]string{{"e", root.ID, "", "reply"}, {"e", root.ID, "", "root"}},
		"reply two",
		now.Add(-30*time.Minute),
	)

	for _, event := range []model.Event{root, replyOne, replyTwo} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.UpdateThreadProjection(ctx, event.ID); err != nil {
			t.Fatalf("project thread for %s: %v", event.ID, err)
		}
	}

	if _, err := pool.Exec(ctx, `
		UPDATE thread_summaries
		SET reply_count = 0,
		    participant_count = 1,
		    max_depth = 0,
		    last_activity_at = $2,
		    replies_24h = 0,
		    replies_7d = 0,
		    derivation_version = 1
		WHERE root_event_id = $1
	`, root.ID, root.CreatedAt); err != nil {
		t.Fatalf("mutate thread summary for rebuild precondition: %v", err)
	}

	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationThreadSummary,
		TargetVersion:  2,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger thread summary rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("execute thread summary rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationThreadSummary, 2, 2)

	var replyCount int64
	var participantCount int
	var maxDepth int
	var replies24h int64
	var replies7d int64
	if err := pool.QueryRow(ctx, `
		SELECT reply_count, participant_count, max_depth, replies_24h, replies_7d
		FROM thread_summaries
		WHERE root_event_id = $1
	`, root.ID).Scan(&replyCount, &participantCount, &maxDepth, &replies24h, &replies7d); err != nil {
		t.Fatalf("read rebuilt thread summary: %v", err)
	}
	if replyCount != 2 || participantCount != 3 || maxDepth != 1 {
		t.Fatalf("unexpected rebuilt summary: replies=%d participants=%d depth=%d", replyCount, participantCount, maxDepth)
	}
	if replies24h != 2 || replies7d != 2 {
		t.Fatalf("unexpected rebuilt velocity hints: 24h=%d 7d=%d", replies24h, replies7d)
	}
}

// Reply weights are the hot-conversation velocity inputs: unique repliers
// only, excluding the root author. A root author replying to their own
// thread in a loop, or one account posting many replies, must not buy
// weight — while the raw reply counters still show the real reply totals.
func TestThreadSummaryProjection_ReplyWeightsDedupeAndExcludeRootAuthor(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	rootID := "thread_weight_root"
	replyTags := [][]string{{"e", rootID, "", "reply"}, {"e", rootID, "", "root"}}
	events := []model.Event{
		newEventForTest(rootID, "weight_root_author", now.Add(-3*time.Hour).Unix(), 1, nil, "root", now.Add(-3*time.Hour)),
		// Self-replies from the root author: count in raw counters, zero weight.
		newEventForTest("thread_weight_self_1", "weight_root_author", now.Add(-2*time.Hour).Unix(), 1, replyTags, "self bump 1", now.Add(-2*time.Hour)),
		newEventForTest("thread_weight_self_2", "weight_root_author", now.Add(-110*time.Minute).Unix(), 1, replyTags, "self bump 2", now.Add(-110*time.Minute)),
		// Two replies from the same account: deduped to one weighted vote.
		newEventForTest("thread_weight_dup_1", "weight_replier_dup", now.Add(-100*time.Minute).Unix(), 1, replyTags, "dup 1", now.Add(-100*time.Minute)),
		newEventForTest("thread_weight_dup_2", "weight_replier_dup", now.Add(-90*time.Minute).Unix(), 1, replyTags, "dup 2", now.Add(-90*time.Minute)),
		// One distinct replier.
		newEventForTest("thread_weight_other", "weight_replier_other", now.Add(-80*time.Minute).Unix(), 1, replyTags, "other", now.Add(-80*time.Minute)),
	}
	for _, event := range events {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.UpdateThreadProjection(ctx, event.ID); err != nil {
			t.Fatalf("project thread for %s: %v", event.ID, err)
		}
	}

	var replies24h int64
	var replyWeight24h, replyWeight7d float64
	if err := pool.QueryRow(ctx, `
		SELECT replies_24h, reply_weight_24h, reply_weight_7d
		FROM thread_summaries
		WHERE root_event_id = $1
	`, rootID).Scan(&replies24h, &replyWeight24h, &replyWeight7d); err != nil {
		t.Fatalf("read thread summary weights: %v", err)
	}
	if replies24h != 5 {
		t.Fatalf("expected raw replies_24h=5 (display counters keep counting events), got %d", replies24h)
	}
	// Two unique non-root-author repliers, trust weighting off => 1.0 each.
	if replyWeight24h != 2.0 || replyWeight7d != 2.0 {
		t.Fatalf("expected reply weights 2.0/2.0, got %f/%f", replyWeight24h, replyWeight7d)
	}
}
