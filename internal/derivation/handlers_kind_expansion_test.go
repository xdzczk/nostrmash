package derivation_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectCommentEdges_RootAndParent(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 4, 4, 18, 0, 0, 0, time.UTC)
	author := strings.Repeat("ab", 32)
	root := newEventForTest("root_evt", author, 1000, 1, nil, "root", base)
	parent := newEventForTest("parent_evt", author, 1001, 1, [][]string{{"e", "root_evt", "", "root"}}, "parent", base.Add(time.Second))
	comment := newEventForTest("comment_evt", author, 1002, 1111, [][]string{{"E", "root_evt"}, {"e", "parent_evt"}, {"K", "1"}}, "nice", base.Add(2*time.Second))
	for _, event := range []model.Event{root, parent, comment} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
	}
	if err := handlers.ProjectCommentEdges(ctx, comment.ID); err != nil {
		t.Fatalf("project comment edges: %v", err)
	}
	var rootID, parentID string
	if err := pool.QueryRow(ctx, `
		SELECT root_event_id, parent_event_id FROM comment_edges WHERE source_event_id = $1
	`, comment.ID).Scan(&rootID, &parentID); err != nil {
		t.Fatalf("load comment edge: %v", err)
	}
	if rootID != "root_evt" || parentID != "parent_evt" {
		t.Fatalf("comment edge root=%q parent=%q", rootID, parentID)
	}
}

func TestProjectRepostCounts_Kind16Address(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 4, 4, 19, 0, 0, 0, time.UTC)
	pubkey := strings.Repeat("cd", 32)
	article := newEventForTest("article_evt", pubkey, 2000, 30023, [][]string{{"d", "post"}}, "article", base)
	repost := newEventForTest("repost16", pubkey, 2001, 16, [][]string{{"a", "30023:" + pubkey + ":post"}, {"p", pubkey}}, "", base.Add(time.Second))
	for _, event := range []model.Event{article, repost} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert %s: %v", event.ID, err)
		}
	}
	if err := handlers.UpdateReplaceableState(ctx, article.ID); err != nil {
		t.Fatalf("replaceable state: %v", err)
	}
	if err := handlers.ProjectRepostCounts(ctx, repost.ID); err != nil {
		t.Fatalf("project reposts: %v", err)
	}
	counts, err := pgStore.GetEventCounts(ctx, article.ID)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts.RepostCount != 1 {
		t.Fatalf("repost count = %d, want 1", counts.RepostCount)
	}
}
