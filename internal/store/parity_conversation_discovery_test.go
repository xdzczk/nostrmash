package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetHotConversations_WindowsAndOrdering(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	rootA := newThreadEventForHotConversations("hot_root_a", "author_a", now.Add(-10*time.Hour), nil, "root a")
	rootB := newThreadEventForHotConversations("hot_root_b", "author_b", now.Add(-6*24*time.Hour), nil, "root b")
	events := []model.Event{
		rootA,
		rootB,
		newThreadEventForHotConversations("hot_a_reply_1", "author_a_1", now.Add(-3*time.Hour),
			[][]string{{"e", rootA.ID, "", "reply"}, {"e", rootA.ID, "", "root"}}, "a reply 1"),
		newThreadEventForHotConversations("hot_a_reply_2", "author_a_2", now.Add(-2*time.Hour),
			[][]string{{"e", rootA.ID, "", "reply"}, {"e", rootA.ID, "", "root"}}, "a reply 2"),
		newThreadEventForHotConversations("hot_b_reply_1", "author_b_1", now.Add(-5*24*time.Hour),
			[][]string{{"e", rootB.ID, "", "reply"}, {"e", rootB.ID, "", "root"}}, "b reply 1"),
		newThreadEventForHotConversations("hot_b_reply_2", "author_b_2", now.Add(-4*24*time.Hour),
			[][]string{{"e", rootB.ID, "", "reply"}, {"e", rootB.ID, "", "root"}}, "b reply 2"),
		newThreadEventForHotConversations("hot_b_reply_3", "author_b_3", now.Add(-2*24*time.Hour),
			[][]string{{"e", rootB.ID, "", "reply"}, {"e", rootB.ID, "", "root"}}, "b reply 3"),
		newThreadEventForHotConversations("hot_b_reply_4", "author_b_4", now.Add(-20*time.Hour),
			[][]string{{"e", rootB.ID, "", "reply"}, {"e", rootB.ID, "", "root"}}, "b reply 4"),
	}

	for _, event := range events {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := s.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.UpdateThreadProjection(ctx, event.ID); err != nil {
			t.Fatalf("project thread for %s: %v", event.ID, err)
		}
	}

	last24h, err := s.GetHotConversations(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetHotConversations 24h: %v", err)
	}
	if len(last24h) != 2 {
		t.Fatalf("unexpected 24h hot conversation count: got=%d want=2", len(last24h))
	}
	if last24h[0].RootEventID != rootA.ID {
		t.Fatalf("expected root_a first in 24h, got %#v", last24h[0])
	}
	if last24h[0].Replies24h != 2 {
		t.Fatalf("unexpected 24h replies for root_a: got=%d want=2", last24h[0].Replies24h)
	}

	last7d, err := s.GetHotConversations(ctx, 7*24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetHotConversations 7d: %v", err)
	}
	if len(last7d) != 2 {
		t.Fatalf("unexpected 7d hot conversation count: got=%d want=2", len(last7d))
	}
	if last7d[0].RootEventID != rootB.ID {
		t.Fatalf("expected root_b first in 7d, got %#v", last7d[0])
	}
	if last7d[0].Replies7d != 4 {
		t.Fatalf("unexpected 7d replies for root_b: got=%d want=4", last7d[0].Replies7d)
	}
	if last7d[0].VelocityScore <= last7d[1].VelocityScore {
		t.Fatalf("expected descending velocity score, got first=%f second=%f", last7d[0].VelocityScore, last7d[1].VelocityScore)
	}
}

func newThreadEventForHotConversations(id, pubkey string, ts time.Time, tags [][]string, content string) model.Event {
	createdAt := ts.Unix()
	raw, _ := json.Marshal(map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       1,
		"tags":       tags,
		"content":    content,
		"sig":        "sig_" + id,
	})
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        1,
		Sig:         "sig_" + id,
		Content:     content,
		RawJSON:     raw,
		FirstSeenAt: ts,
		InsertedAt:  ts,
	}
}
