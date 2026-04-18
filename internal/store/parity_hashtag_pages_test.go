package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestHashtagSummaryAndMissingBehavior(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newTaggedEvent("sum_evt_1", "author_a", now.Add(-1*time.Hour), "nostr"),
		newTaggedEvent("sum_evt_2", "author_b", now.Add(-5*time.Hour), "nostr"),
		newTaggedEvent("sum_evt_3", "author_b", now.Add(-9*24*time.Hour), "nostr"),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectEventHashtags(ctx, event.ID); err != nil {
			t.Fatalf("project hashtags %s: %v", event.ID, err)
		}
		if err := handlers.ProjectNoteDiscoveryStats(ctx, event.ID); err != nil {
			t.Fatalf("project note discovery stats %s: %v", event.ID, err)
		}
	}

	summary, err := pgStore.GetHashtagSummary(ctx, "  #Nostr ")
	if err != nil {
		t.Fatalf("GetHashtagSummary: %v", err)
	}
	if summary.Hashtag != "nostr" {
		t.Fatalf("unexpected normalized hashtag: got=%q want=nostr", summary.Hashtag)
	}
	if summary.Activity.All.EventCount != 3 || summary.Activity.All.UniqueAuthors != 2 {
		t.Fatalf("unexpected all-time hashtag activity: %#v", summary.Activity.All)
	}
	if summary.Activity.Last24h.EventCount != 2 || summary.Activity.Last24h.UniqueAuthors != 2 {
		t.Fatalf("unexpected 24h hashtag activity: %#v", summary.Activity.Last24h)
	}

	_, err = pgStore.GetHashtagSummary(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing hashtag, got %v", err)
	}
}

func TestHashtagNotesLatestVsTopAndRelated(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newTaggedEvent("notes_evt_1", "author_a", now.Add(-6*time.Hour), "nostr"),
		newTaggedEvent("notes_evt_2", "author_b", now.Add(-1*time.Hour), "nostr"),
		newTaggedEvent("notes_evt_3", "author_c", now.Add(-2*time.Hour), "bitcoin"),
		newMultiTaggedEvent("notes_evt_4", "author_d", now.Add(-3*time.Hour), []string{"nostr", "bitcoin"}),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectEventHashtags(ctx, event.ID); err != nil {
			t.Fatalf("project hashtags %s: %v", event.ID, err)
		}
		if err := handlers.ProjectNoteDiscoveryStats(ctx, event.ID); err != nil {
			t.Fatalf("project note discovery stats %s: %v", event.ID, err)
		}
	}

	// Older note gets stronger engagement so top order differs from latest order.
	if _, err := pool.Exec(ctx, `UPDATE note_discovery_stats SET score_7d = 50 WHERE event_id = 'notes_evt_1'`); err != nil {
		t.Fatalf("boost score for notes_evt_1: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE note_discovery_stats SET score_7d = 1 WHERE event_id = 'notes_evt_2'`); err != nil {
		t.Fatalf("lower score for notes_evt_2: %v", err)
	}

	latest, err := pgStore.GetHashtagNotes(ctx, "nostr", "latest", "7d", 10, 0)
	if err != nil {
		t.Fatalf("GetHashtagNotes latest: %v", err)
	}
	top, err := pgStore.GetHashtagNotes(ctx, "nostr", "top", "7d", 10, 0)
	if err != nil {
		t.Fatalf("GetHashtagNotes top: %v", err)
	}
	if len(latest) < 2 || len(top) < 2 {
		t.Fatalf("unexpected hashtag notes count latest=%d top=%d", len(latest), len(top))
	}
	if latest[0].EventID == top[0].EventID {
		t.Fatalf("expected latest and top ordering to differ, got same head event %s", latest[0].EventID)
	}

	related, err := pgStore.GetRelatedHashtags(ctx, "nostr", 10)
	if err != nil {
		t.Fatalf("GetRelatedHashtags: %v", err)
	}
	if len(related) == 0 || related[0].Hashtag != "bitcoin" {
		t.Fatalf("expected bitcoin as a related hashtag, got %#v", related)
	}
}

func newMultiTaggedEvent(id, pubkey string, ts time.Time, hashtags []string) model.Event {
	if len(hashtags) == 0 {
		return newTaggedEvent(id, pubkey, ts, "nostr")
	}
	tags := make([][]string, 0, len(hashtags))
	for _, hashtag := range hashtags {
		tags = append(tags, []string{"t", hashtag})
	}
	createdAt := ts.Unix()
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
