package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetTrendingHashtags_Aggregates24hAnd7d(t *testing.T) {
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
		newTaggedEvent("hash_evt_1", "author_a", now.Add(-1*time.Hour), "nostr"),
		newTaggedEvent("hash_evt_2", "author_b", now.Add(-2*time.Hour), "nostr"),
		newTaggedEvent("hash_evt_3", "author_a", now.Add(-3*time.Hour), "bitcoin"),
		newTaggedEvent("hash_evt_4", "author_c", now.Add(-26*time.Hour), "nostr"),
		newTaggedEvent("hash_evt_5", "author_d", now.Add(-72*time.Hour), "bitcoin"),
		newTaggedEvent("hash_evt_6", "author_e", now.Add(-9*24*time.Hour), "old"),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectEventHashtags(ctx, event.ID); err != nil {
			t.Fatalf("project hashtags %s: %v", event.ID, err)
		}
	}

	last24h, err := pgStore.GetTrendingHashtags(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingHashtags 24h: %v", err)
	}
	if len(last24h) != 2 {
		t.Fatalf("unexpected 24h hashtag count: got=%d want=2", len(last24h))
	}
	if last24h[0].Hashtag != "nostr" || last24h[0].EventCount != 2 || last24h[0].UniqueAuthors != 2 {
		t.Fatalf("unexpected top 24h hashtag row: %#v", last24h[0])
	}
	if last24h[1].Hashtag != "bitcoin" || last24h[1].EventCount != 1 || last24h[1].UniqueAuthors != 1 {
		t.Fatalf("unexpected second 24h hashtag row: %#v", last24h[1])
	}

	last7d, err := pgStore.GetTrendingHashtags(ctx, 7*24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingHashtags 7d: %v", err)
	}
	if len(last7d) != 2 {
		t.Fatalf("unexpected 7d hashtag count: got=%d want=2", len(last7d))
	}
	if last7d[0].Hashtag != "nostr" || last7d[0].EventCount != 3 || last7d[0].UniqueAuthors != 3 {
		t.Fatalf("unexpected top 7d hashtag row: %#v", last7d[0])
	}
	if last7d[1].Hashtag != "bitcoin" || last7d[1].EventCount != 2 || last7d[1].UniqueAuthors != 2 {
		t.Fatalf("unexpected second 7d hashtag row: %#v", last7d[1])
	}
}

func TestGetTrendingHashtags_TieBreaksByUniqueAuthors(t *testing.T) {
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
		newTaggedEvent("tie_evt_1", "author_a", now.Add(-1*time.Hour), "alpha"),
		newTaggedEvent("tie_evt_2", "author_a", now.Add(-2*time.Hour), "alpha"),
		newTaggedEvent("tie_evt_3", "author_b", now.Add(-3*time.Hour), "beta"),
		newTaggedEvent("tie_evt_4", "author_c", now.Add(-4*time.Hour), "beta"),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectEventHashtags(ctx, event.ID); err != nil {
			t.Fatalf("project hashtags %s: %v", event.ID, err)
		}
	}

	out, err := pgStore.GetTrendingHashtags(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingHashtags: %v", err)
	}
	if len(out) < 2 {
		t.Fatalf("unexpected result count: got=%d want>=2", len(out))
	}
	if out[0].Hashtag != "beta" {
		t.Fatalf("expected beta to win tie-break by unique authors, got %#v", out[0])
	}
}

func newTaggedEvent(id, pubkey string, ts time.Time, hashtag string) model.Event {
	createdAt := ts.Unix()
	tags := [][]string{{"t", hashtag}}
	raw, _ := json.Marshal(map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       1,
		"tags":       tags,
		"content":    "hashtag test",
		"sig":        "sig_" + id,
	})
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        1,
		Sig:         "sig_" + id,
		Content:     "hashtag test",
		RawJSON:     raw,
		FirstSeenAt: ts,
		InsertedAt:  ts,
	}
}

func extractTagsForStoreTest(t *testing.T, raw json.RawMessage) [][]string {
	t.Helper()
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode tags from raw event: %v", err)
	}
	return payload.Tags
}
