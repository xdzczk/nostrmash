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

func TestProjectionRebuildScopes_ReplyCounts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 20, 0, 0, 0, time.UTC)

	target := newEventForTest("rebuild_target", "author_target", 1000, 1, nil, "{}", baseTime)
	replyA1 := newEventForTest(
		"reply_a1",
		"author_a",
		1001,
		1,
		[][]string{{"e", "rebuild_target", "", "reply"}},
		`{"content":"a1"}`,
		baseTime.Add(1*time.Second),
	)
	replyB := newEventForTest(
		"reply_b",
		"author_b",
		1002,
		1,
		[][]string{{"e", "rebuild_target", "", "reply"}},
		`{"content":"b"}`,
		baseTime.Add(2*time.Second),
	)
	replyA2 := newEventForTest(
		"reply_a2",
		"author_a",
		1003,
		1,
		[][]string{{"e", "rebuild_target", "", "reply"}},
		`{"content":"a2"}`,
		baseTime.Add(3*time.Second),
	)

	for _, event := range []model.Event{target, replyA1, replyB, replyA2} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}
	for _, eventID := range []string{replyA1.ID, replyB.ID, replyA2.ID} {
		if err := handlers.ProjectReplyCounts(ctx, eventID); err != nil {
			t.Fatalf("project reply counts %s: %v", eventID, err)
		}
	}

	// Rebuild to a version above the compiled active version so scoped
	// rebuilds can demonstrate partial contribution upgrades.
	const rebuildTarget = derivation.ReplyCountsVersion + 1
	baseVersion := derivation.ReplyCountsVersion

	runEvent, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  rebuildTarget,
		Scope: derivation.ProjectionRebuildScope{
			Type:    derivation.RebuildScopeEvent,
			EventID: replyA1.ID,
		},
	})
	if err != nil {
		t.Fatalf("trigger event-scoped rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, runEvent.ID); err != nil {
		t.Fatalf("execute event-scoped rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, runEvent.ID)
	assertReplyContributionVersions(t, ctx, pool, map[string]int{
		replyA1.ID: rebuildTarget,
		replyB.ID:  baseVersion,
		replyA2.ID: baseVersion,
	})
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationReplyCounts, baseVersion, rebuildTarget)

	runPubkey, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  rebuildTarget,
		Scope: derivation.ProjectionRebuildScope{
			Type:   derivation.RebuildScopePubkey,
			Pubkey: "author_a",
		},
	})
	if err != nil {
		t.Fatalf("trigger pubkey-scoped rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, runPubkey.ID); err != nil {
		t.Fatalf("execute pubkey-scoped rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, runPubkey.ID)
	assertReplyContributionVersions(t, ctx, pool, map[string]int{
		replyA1.ID: rebuildTarget,
		replyB.ID:  baseVersion,
		replyA2.ID: rebuildTarget,
	})
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationReplyCounts, baseVersion, rebuildTarget)

	start := int64(1002)
	end := int64(1002)
	runTimeRange, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  rebuildTarget,
		Scope: derivation.ProjectionRebuildScope{
			Type:           derivation.RebuildScopeTimeRange,
			StartCreatedAt: &start,
			EndCreatedAt:   &end,
		},
	})
	if err != nil {
		t.Fatalf("trigger time-range rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, runTimeRange.ID); err != nil {
		t.Fatalf("execute time-range rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, runTimeRange.ID)
	assertReplyContributionVersions(t, ctx, pool, map[string]int{
		replyA1.ID: rebuildTarget,
		replyB.ID:  rebuildTarget,
		replyA2.ID: rebuildTarget,
	})
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationReplyCounts, baseVersion, rebuildTarget)

	runFull, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  rebuildTarget,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger full rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, runFull.ID); err != nil {
		t.Fatalf("execute full rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, runFull.ID)
	assertReplyContributionVersions(t, ctx, pool, map[string]int{
		replyA1.ID: rebuildTarget,
		replyB.ID:  rebuildTarget,
		replyA2.ID: rebuildTarget,
	})
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationReplyCounts, rebuildTarget, rebuildTarget)
}
