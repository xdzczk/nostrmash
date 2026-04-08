package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestThreadSummaryProjection_RebuildCorrectness(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

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
