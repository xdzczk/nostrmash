package derivation_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestDeriveEventRelationships_UnmarkedV1Semantics(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	tags := [][]string{
		{"e", "event_root"},
		{"e", "event_mention"},
		{"e", "event_reply"},
		{"p", "pub_root"},
		{"p", "pub_mention"},
		{"p", "pub_reply"},
	}
	raw, err := json.Marshal(map[string]any{
		"id":         "source_event_1",
		"pubkey":     "source_pub",
		"created_at": 1000,
		"kind":       1,
		"tags":       tags,
		"content":    "hello",
		"sig":        "sig_source_1",
	})
	if err != nil {
		t.Fatalf("marshal source event raw json: %v", err)
	}
	event := model.Event{
		ID:          "source_event_1",
		Pubkey:      "source_pub",
		CreatedAt:   1000,
		Kind:        1,
		Sig:         "sig_source_1",
		Content:     "hello",
		RawJSON:     raw,
		FirstSeenAt: time.Date(2026, 4, 4, 16, 0, 0, 0, time.UTC),
		InsertedAt:  time.Date(2026, 4, 4, 16, 0, 0, 0, time.UTC),
	}
	if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert source event: %v", err)
	}

	if err := handlers.DeriveEventRelationships(ctx, event.ID); err != nil {
		t.Fatalf("derive event relationships: %v", err)
	}
	// Idempotency: a second run should converge to same rows.
	if err := handlers.DeriveEventRelationships(ctx, event.ID); err != nil {
		t.Fatalf("derive event relationships second run: %v", err)
	}

	eventRefRows, err := readEventRefRows(ctx, pool, event.ID)
	if err != nil {
		t.Fatalf("read event references: %v", err)
	}
	expectedEventRefs := []refRow{
		{referenced: "event_root", relation: "root", tagIndex: 0},
		{referenced: "event_mention", relation: "mention", tagIndex: 1},
		{referenced: "event_reply", relation: "reply", tagIndex: 2},
	}
	assertRefRowsEqual(t, eventRefRows, expectedEventRefs)

	pubkeyRefRows, err := readPubkeyRefRows(ctx, pool, event.ID)
	if err != nil {
		t.Fatalf("read pubkey references: %v", err)
	}
	expectedPubkeyRefs := []refRow{
		{referenced: "pub_root", relation: "root", tagIndex: 3},
		{referenced: "pub_mention", relation: "mention", tagIndex: 4},
		{referenced: "pub_reply", relation: "reply", tagIndex: 5},
	}
	assertRefRowsEqual(t, pubkeyRefRows, expectedPubkeyRefs)
}

func TestUpdateReplaceableState_TieBreakByEventID(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 16, 30, 0, 0, time.UTC)
	events := []model.Event{
		{
			ID:          "aaaaaaaa",
			Pubkey:      "pub_replaceable",
			CreatedAt:   1000,
			Kind:        0,
			Sig:         "sig_a",
			Content:     "a",
			RawJSON:     json.RawMessage(`{"id":"aaaaaaaa","kind":0,"tags":[]}`),
			FirstSeenAt: baseTime,
			InsertedAt:  baseTime,
		},
		{
			ID:          "bbbbbbbb",
			Pubkey:      "pub_replaceable",
			CreatedAt:   1000,
			Kind:        0,
			Sig:         "sig_b",
			Content:     "b",
			RawJSON:     json.RawMessage(`{"id":"bbbbbbbb","kind":0,"tags":[]}`),
			FirstSeenAt: baseTime.Add(1 * time.Second),
			InsertedAt:  baseTime.Add(1 * time.Second),
		},
		{
			ID:          "00000000",
			Pubkey:      "pub_replaceable",
			CreatedAt:   1001,
			Kind:        0,
			Sig:         "sig_c",
			Content:     "c",
			RawJSON:     json.RawMessage(`{"id":"00000000","kind":0,"tags":[]}`),
			FirstSeenAt: baseTime.Add(2 * time.Second),
			InsertedAt:  baseTime.Add(2 * time.Second),
		},
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, nil, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}

	if err := handlers.UpdateReplaceableState(ctx, "aaaaaaaa"); err != nil {
		t.Fatalf("derive replaceable state a: %v", err)
	}
	if err := handlers.UpdateReplaceableState(ctx, "bbbbbbbb"); err != nil {
		t.Fatalf("derive replaceable state b: %v", err)
	}
	// Lower event id with same created_at should not replace bbbbbbbb.
	if err := handlers.UpdateReplaceableState(ctx, "aaaaaaaa"); err != nil {
		t.Fatalf("derive replaceable state a again: %v", err)
	}

	var winnerID string
	var winnerCreatedAt int64
	if err := pool.QueryRow(ctx, `
		SELECT event_id, created_at
		FROM replaceable_state
		WHERE pubkey = $1 AND kind = $2 AND d_tag = ''
	`, "pub_replaceable", 0).Scan(&winnerID, &winnerCreatedAt); err != nil {
		t.Fatalf("query replaceable winner after tie: %v", err)
	}
	if winnerID != "bbbbbbbb" || winnerCreatedAt != 1000 {
		t.Fatalf("unexpected winner after tie-break: id=%s created_at=%d", winnerID, winnerCreatedAt)
	}

	if err := handlers.UpdateReplaceableState(ctx, "00000000"); err != nil {
		t.Fatalf("derive replaceable state newer event: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT event_id, created_at
		FROM replaceable_state
		WHERE pubkey = $1 AND kind = $2 AND d_tag = ''
	`, "pub_replaceable", 0).Scan(&winnerID, &winnerCreatedAt); err != nil {
		t.Fatalf("query replaceable winner after newer: %v", err)
	}
	if winnerID != "00000000" || winnerCreatedAt != 1001 {
		t.Fatalf("unexpected winner after newer event: id=%s created_at=%d", winnerID, winnerCreatedAt)
	}
}

func TestProjectAuthorRecentEvent_OrderByCreatedAtDescIDDesc(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 17, 0, 0, 0, time.UTC)

	events := []model.Event{
		newEventForTest("mmmm", "author_ordering", 1002, 1, nil, "{}", baseTime.Add(1*time.Second)),
		newEventForTest("aaaa", "author_ordering", 1000, 1, nil, "{}", baseTime.Add(2*time.Second)),
		newEventForTest("zzzz", "author_ordering", 1000, 1, nil, "{}", baseTime.Add(3*time.Second)),
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, nil, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectAuthorRecentEvent(ctx, event.ID); err != nil {
			t.Fatalf("project author_recent_events for %s: %v", event.ID, err)
		}
	}

	recent, err := pgStore.GetAuthorRecentEvents(ctx, "author_ordering", 10)
	if err != nil {
		t.Fatalf("get author recent events: %v", err)
	}
	if len(recent) != 3 {
		t.Fatalf("unexpected event count: got=%d want=3", len(recent))
	}

	gotIDs := make([]string, 0, len(recent))
	for _, raw := range recent {
		var decoded struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode recent event: %v", err)
		}
		gotIDs = append(gotIDs, decoded.ID)
	}
	wantIDs := []string{"mmmm", "zzzz", "aaaa"}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("unexpected author ordering: got=%v want=%v", gotIDs, wantIDs)
		}
	}
}

func TestProjectCounts_ReplyReactionRepost(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

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

func TestThreadProjection_RepairsMissingParentWhenReferenceArrives(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 19, 0, 0, 0, time.UTC)

	child := newEventForTest(
		"thread_child_missing_parent",
		"author_child",
		1001,
		1,
		[][]string{{"e", "thread_parent_late", "", "reply"}},
		`{"content":"child"}`,
		baseTime,
	)
	if err := pgStore.InsertCanonicalEvent(ctx, child, extractTagsFromRaw(t, child.RawJSON), "wss://relay.one", child.FirstSeenAt); err != nil {
		t.Fatalf("insert child event: %v", err)
	}
	if err := handlers.UpdateThreadProjection(ctx, child.ID); err != nil {
		t.Fatalf("project child thread edge with missing parent: %v", err)
	}

	var parentMissing bool
	if err := pool.QueryRow(ctx, `
		SELECT parent_missing
		FROM thread_edges
		WHERE child_event_id = $1
	`, child.ID).Scan(&parentMissing); err != nil {
		t.Fatalf("query projected thread edge: %v", err)
	}
	if !parentMissing {
		t.Fatalf("expected parent_missing=true before repair")
	}

	var unresolvedCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM unresolved_thread_references
		WHERE source_event_id = $1 AND missing_event_id = $2
	`, child.ID, "thread_parent_late").Scan(&unresolvedCount); err != nil {
		t.Fatalf("count unresolved references before repair: %v", err)
	}
	if unresolvedCount != 1 {
		t.Fatalf("expected one unresolved reference, got %d", unresolvedCount)
	}

	parent := newEventForTest(
		"thread_parent_late",
		"author_parent",
		1000,
		1,
		nil,
		`{"content":"parent"}`,
		baseTime.Add(1*time.Second),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, parent, nil, "wss://relay.one", parent.FirstSeenAt); err != nil {
		t.Fatalf("insert late parent: %v", err)
	}
	if err := handlers.RepairUnresolvedReferences(ctx, parent.ID); err != nil {
		t.Fatalf("repair unresolved references: %v", err)
	}
	if err := handlers.UpdateThreadProjection(ctx, child.ID); err != nil {
		t.Fatalf("reproject child after repair: %v", err)
	}

	if err := pool.QueryRow(ctx, `
		SELECT parent_missing
		FROM thread_edges
		WHERE child_event_id = $1
	`, child.ID).Scan(&parentMissing); err != nil {
		t.Fatalf("query thread edge after repair: %v", err)
	}
	if parentMissing {
		t.Fatalf("expected parent_missing=false after repair")
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM unresolved_thread_references
		WHERE source_event_id = $1
	`, child.ID).Scan(&unresolvedCount); err != nil {
		t.Fatalf("count unresolved references after repair: %v", err)
	}
	if unresolvedCount != 0 {
		t.Fatalf("expected unresolved references to be cleared, got %d", unresolvedCount)
	}
}

func TestProjectionRebuildScopes_ReplyCounts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

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

	runEvent, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  2,
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
		replyA1.ID: 2,
		replyB.ID:  1,
		replyA2.ID: 1,
	})
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationReplyCounts, 1, 2)

	runPubkey, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  2,
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
		replyA1.ID: 2,
		replyB.ID:  1,
		replyA2.ID: 2,
	})
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationReplyCounts, 1, 2)

	start := int64(1002)
	end := int64(1002)
	runTimeRange, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  2,
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
		replyA1.ID: 2,
		replyB.ID:  2,
		replyA2.ID: 2,
	})
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationReplyCounts, 1, 2)

	runFull, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  2,
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
		replyA1.ID: 2,
		replyB.ID:  2,
		replyA2.ID: 2,
	})
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationReplyCounts, 2, 2)
}

func TestProjectionRebuildRun_FailedRunsTrackErrorAndRetryAttempts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  2,
		Scope: derivation.ProjectionRebuildScope{
			Type:    derivation.RebuildScopeEvent,
			EventID: "missing_event",
		},
	})
	if err != nil {
		t.Fatalf("trigger missing-event rebuild: %v", err)
	}

	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err == nil {
		t.Fatalf("expected first rebuild attempt to fail")
	}
	first, err := handlers.GetProjectionRebuildRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after first failure: %v", err)
	}
	if first.Status != derivation.RebuildStatusFailed {
		t.Fatalf("expected failed status after first attempt, got %q", first.Status)
	}
	if first.Attempts != 1 {
		t.Fatalf("expected attempts=1 after first failure, got %d", first.Attempts)
	}
	if first.StartedAt == nil || first.FinishedAt == nil {
		t.Fatalf("expected started_at and finished_at to be set after first failure")
	}
	if first.LastError == nil || strings.TrimSpace(*first.LastError) == "" {
		t.Fatalf("expected last_error to be populated after first failure")
	}

	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err == nil {
		t.Fatalf("expected second rebuild attempt to fail")
	}
	second, err := handlers.GetProjectionRebuildRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after second failure: %v", err)
	}
	if second.Status != derivation.RebuildStatusFailed {
		t.Fatalf("expected failed status after second attempt, got %q", second.Status)
	}
	if second.Attempts != 2 {
		t.Fatalf("expected attempts=2 after second failure, got %d", second.Attempts)
	}
	if second.LastError == nil || strings.TrimSpace(*second.LastError) == "" {
		t.Fatalf("expected last_error to remain populated after second failure")
	}
}

func newEventForTest(
	id string,
	pubkey string,
	createdAt int64,
	kind int,
	tags [][]string,
	content string,
	firstSeenAt time.Time,
) model.Event {
	payload := map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       tags,
		"content":    content,
		"sig":        "sig_" + id,
	}
	raw, _ := json.Marshal(payload)
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        kind,
		Sig:         "sig_" + id,
		Content:     content,
		RawJSON:     raw,
		FirstSeenAt: firstSeenAt,
		InsertedAt:  firstSeenAt,
	}
}

func extractTagsFromRaw(t *testing.T, raw json.RawMessage) [][]string {
	t.Helper()
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode tags from raw event: %v", err)
	}
	return payload.Tags
}

func assertRebuildRunSucceeded(t *testing.T, ctx context.Context, handlers *derivation.Handlers, runID int64) {
	t.Helper()
	run, err := handlers.GetProjectionRebuildRun(ctx, runID)
	if err != nil {
		t.Fatalf("get rebuild run %d: %v", runID, err)
	}
	if run.Status != derivation.RebuildStatusSucceeded {
		t.Fatalf("expected run %d status=%q, got %q", runID, derivation.RebuildStatusSucceeded, run.Status)
	}
	if run.StartedAt == nil || run.FinishedAt == nil {
		t.Fatalf("expected run %d started_at and finished_at to be set", runID)
	}
	if run.LastError != nil {
		t.Fatalf("expected run %d last_error to be nil, got %q", runID, *run.LastError)
	}
}

func assertReplyContributionVersions(t *testing.T, ctx context.Context, pool *pgxpool.Pool, expected map[string]int) {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT source_event_id, derivation_version
		FROM reply_count_contributions
	`)
	if err != nil {
		t.Fatalf("query reply contribution versions: %v", err)
	}
	defer rows.Close()

	got := make(map[string]int, len(expected))
	for rows.Next() {
		var sourceEventID string
		var version int
		if err := rows.Scan(&sourceEventID, &version); err != nil {
			t.Fatalf("scan reply contribution version row: %v", err)
		}
		got[sourceEventID] = version
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read reply contribution version rows: %v", err)
	}

	if len(got) != len(expected) {
		t.Fatalf("unexpected reply contribution row count: got=%d want=%d", len(got), len(expected))
	}
	for eventID, expectedVersion := range expected {
		gotVersion, ok := got[eventID]
		if !ok {
			t.Fatalf("missing expected contribution row for %s", eventID)
		}
		if gotVersion != expectedVersion {
			t.Fatalf("unexpected derivation_version for %s: got=%d want=%d", eventID, gotVersion, expectedVersion)
		}
	}
}

func assertActiveAndTargetVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, derivationName string, expectedActive, expectedTarget int) {
	t.Helper()
	var active int
	var target int
	if err := pool.QueryRow(ctx, `
		SELECT active_version, target_version
		FROM derivation_active_versions
		WHERE derivation_name = $1
	`, derivationName).Scan(&active, &target); err != nil {
		t.Fatalf("query derivation active/target versions for %s: %v", derivationName, err)
	}
	if active != expectedActive || target != expectedTarget {
		t.Fatalf(
			"unexpected derivation versions for %s: active=%d target=%d want_active=%d want_target=%d",
			derivationName,
			active,
			target,
			expectedActive,
			expectedTarget,
		)
	}
}

func readEventRefRows(ctx context.Context, pool *pgxpool.Pool, eventID string) ([]refRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT referenced_event_id, relation, tag_index
		FROM event_references
		WHERE source_event_id = $1
		ORDER BY tag_index ASC
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]refRow, 0)
	for rows.Next() {
		var row refRow
		if err := rows.Scan(&row.referenced, &row.relation, &row.tagIndex); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func readPubkeyRefRows(ctx context.Context, pool *pgxpool.Pool, eventID string) ([]refRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT referenced_pubkey, relation, tag_index
		FROM pubkey_references
		WHERE source_event_id = $1
		ORDER BY tag_index ASC
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]refRow, 0)
	for rows.Next() {
		var row refRow
		if err := rows.Scan(&row.referenced, &row.relation, &row.tagIndex); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

type refRow struct {
	referenced string
	relation   string
	tagIndex   int
}

func assertRefRowsEqual(t *testing.T, got, want []refRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("unexpected row count: got=%d want=%d rows=%#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected row at index %d: got=%#v want=%#v", i, got[i], want[i])
		}
	}
}

func setupSchemaPool(t *testing.T, ctx context.Context, dbURL string) *pgxpool.Pool {
	t.Helper()

	adminPool, err := store.OpenPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schemaName := fmt.Sprintf("test_derivations_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, quotedSchema)); err != nil {
		adminPool.Close()
		t.Fatalf("create schema %s: %v", schemaName, err)
	}

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		adminPool.Close()
		t.Fatalf("parse pool config: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schemaName

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		adminPool.Close()
		t.Fatalf("open schema pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, quotedSchema))
		adminPool.Close()
	})

	return pool
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()

	candidates := []string{
		os.Getenv("TEST_DATABASE_URL"),
		os.Getenv("DATABASE_URL"),
	}
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		return candidate
	}

	t.Skip("set TEST_DATABASE_URL or DATABASE_URL to run derivation integration tests")
	return ""
}
