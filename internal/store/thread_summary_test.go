package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetThreadSummary_Correctness(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	root := model.Event{
		ID:          "thread_summary_root",
		Pubkey:      "author_root",
		CreatedAt:   now.Add(-6 * time.Hour).Unix(),
		Kind:        1,
		Sig:         "sig_root",
		Content:     "root",
		RawJSON:     json.RawMessage(`{"id":"thread_summary_root","kind":1,"tags":[]}`),
		FirstSeenAt: now.Add(-6 * time.Hour),
		InsertedAt:  now.Add(-6 * time.Hour),
	}
	replyA := model.Event{
		ID:          "thread_summary_reply_a",
		Pubkey:      "author_a",
		CreatedAt:   now.Add(-3 * time.Hour).Unix(),
		Kind:        1,
		Sig:         "sig_reply_a",
		Content:     "reply a",
		RawJSON:     json.RawMessage(`{"id":"thread_summary_reply_a","kind":1,"tags":[["e","thread_summary_root","","reply"],["e","thread_summary_root","","root"]]}`),
		FirstSeenAt: now.Add(-3 * time.Hour),
		InsertedAt:  now.Add(-3 * time.Hour),
	}
	replyB := model.Event{
		ID:          "thread_summary_reply_b",
		Pubkey:      "author_a",
		CreatedAt:   now.Add(-2 * time.Hour).Unix(),
		Kind:        1,
		Sig:         "sig_reply_b",
		Content:     "reply b",
		RawJSON:     json.RawMessage(`{"id":"thread_summary_reply_b","kind":1,"tags":[["e","thread_summary_root","","reply"],["e","thread_summary_root","","root"]]}`),
		FirstSeenAt: now.Add(-2 * time.Hour),
		InsertedAt:  now.Add(-2 * time.Hour),
	}
	nested := model.Event{
		ID:          "thread_summary_nested",
		Pubkey:      "author_b",
		CreatedAt:   now.Add(-30 * time.Minute).Unix(),
		Kind:        1,
		Sig:         "sig_nested",
		Content:     "nested",
		RawJSON:     json.RawMessage(`{"id":"thread_summary_nested","kind":1,"tags":[["e","thread_summary_root","","root"],["e","thread_summary_reply_a","","reply"]]}`),
		FirstSeenAt: now.Add(-30 * time.Minute),
		InsertedAt:  now.Add(-30 * time.Minute),
	}

	for _, event := range []struct {
		evt  model.Event
		tags [][]string
	}{
		{evt: root, tags: nil},
		{evt: replyA, tags: [][]string{{"e", root.ID, "", "reply"}, {"e", root.ID, "", "root"}}},
		{evt: replyB, tags: [][]string{{"e", root.ID, "", "reply"}, {"e", root.ID, "", "root"}}},
		{evt: nested, tags: [][]string{{"e", root.ID, "", "root"}, {"e", replyA.ID, "", "reply"}}},
	} {
		if err := s.InsertCanonicalEvent(ctx, event.evt, event.tags, "wss://relay.one", event.evt.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.evt.ID, err)
		}
		if err := handlers.UpdateThreadProjection(ctx, event.evt.ID); err != nil {
			t.Fatalf("project thread edge for %s: %v", event.evt.ID, err)
		}
	}

	summary, err := s.GetThreadSummary(ctx, root.ID)
	if err != nil {
		t.Fatalf("get thread summary: %v", err)
	}
	if summary.RootEventID != root.ID {
		t.Fatalf("unexpected root id: %s", summary.RootEventID)
	}
	if summary.ReplyCount != 3 {
		t.Fatalf("unexpected reply_count: got %d want 3", summary.ReplyCount)
	}
	if summary.ParticipantCount != 3 {
		t.Fatalf("unexpected participant_count: got %d want 3", summary.ParticipantCount)
	}
	if summary.MaxDepth != 2 {
		t.Fatalf("unexpected max_depth: got %d want 2", summary.MaxDepth)
	}
	if summary.LastActivityAt != nested.CreatedAt {
		t.Fatalf("unexpected last_activity_at: got %d want %d", summary.LastActivityAt, nested.CreatedAt)
	}
	if summary.Replies24h != 3 || summary.Replies7d != 3 {
		t.Fatalf("unexpected velocity hints: 24h=%d 7d=%d", summary.Replies24h, summary.Replies7d)
	}
}

func TestGetThreadSummary_SparseMalformedThreadHandling(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	root := model.Event{
		ID:          "thread_sparse_root",
		Pubkey:      "author_root",
		CreatedAt:   now.Add(-4 * time.Hour).Unix(),
		Kind:        1,
		Sig:         "sig_sparse_root",
		Content:     "root",
		RawJSON:     json.RawMessage(`{"id":"thread_sparse_root","kind":1,"tags":[]}`),
		FirstSeenAt: now.Add(-4 * time.Hour),
		InsertedAt:  now.Add(-4 * time.Hour),
	}
	orphan := model.Event{
		ID:          "thread_sparse_orphan_reply",
		Pubkey:      "author_orphan",
		CreatedAt:   now.Add(-10 * time.Minute).Unix(),
		Kind:        1,
		Sig:         "sig_sparse_orphan",
		Content:     "orphaned but rooted",
		RawJSON:     json.RawMessage(`{"id":"thread_sparse_orphan_reply","kind":1,"tags":[["e","thread_sparse_root","","root"],["e","thread_missing_parent","","reply"]]}`),
		FirstSeenAt: now.Add(-10 * time.Minute),
		InsertedAt:  now.Add(-10 * time.Minute),
	}

	if err := s.InsertCanonicalEvent(ctx, root, nil, "wss://relay.one", root.FirstSeenAt); err != nil {
		t.Fatalf("insert root: %v", err)
	}
	if err := handlers.UpdateThreadProjection(ctx, root.ID); err != nil {
		t.Fatalf("project root: %v", err)
	}
	if err := s.InsertCanonicalEvent(
		ctx,
		orphan,
		[][]string{{"e", root.ID, "", "root"}, {"e", "thread_missing_parent", "", "reply"}},
		"wss://relay.one",
		orphan.FirstSeenAt,
	); err != nil {
		t.Fatalf("insert orphan reply: %v", err)
	}
	if err := handlers.UpdateThreadProjection(ctx, orphan.ID); err != nil {
		t.Fatalf("project orphan reply: %v", err)
	}

	summary, err := s.GetThreadSummary(ctx, root.ID)
	if err != nil {
		t.Fatalf("get sparse thread summary: %v", err)
	}
	if summary.ReplyCount != 1 {
		t.Fatalf("unexpected sparse reply_count: got %d want 1", summary.ReplyCount)
	}
	if summary.ParticipantCount != 2 {
		t.Fatalf("unexpected sparse participant_count: got %d want 2", summary.ParticipantCount)
	}
	if summary.MaxDepth != 0 {
		t.Fatalf("unexpected sparse max_depth: got %d want 0", summary.MaxDepth)
	}
	if summary.LastActivityAt != orphan.CreatedAt {
		t.Fatalf("unexpected sparse last_activity_at: got %d want %d", summary.LastActivityAt, orphan.CreatedAt)
	}
}
