package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetRelatedNotes_BoundedAndRanked(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newNotePageTestEvent("focal_evt", "author_a", now.Add(-1*time.Hour), []string{"nostr"}),
		newNotePageTestEvent("hashtag_evt", "author_b", now.Add(-2*time.Hour), []string{"nostr", "bitcoin"}),
		newNotePageTestEvent("author_evt", "author_a", now.Add(-3*time.Hour), []string{"dev"}),
		newNotePageTestEvent("thread_evt", "author_c", now.Add(-30*time.Minute), []string{"nostr"}),
		newNotePageTestEvent("repost_evt", "author_d", now.Add(-20*time.Minute), []string{"nostr"}),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.test", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}
	for _, eventID := range []string{"focal_evt", "hashtag_evt", "author_evt", "thread_evt", "repost_evt"} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO note_discovery_stats (
				event_id, author_pubkey, created_at, reply_count, repost_count, reaction_count, zap_count, zap_msats, score_24h, score_7d, derivation_version
			)
			SELECT id, pubkey, created_at, 1, 1, 1, 0, 0, 1.0, 1.0, 1
			FROM events
			WHERE id = $1
		`, eventID); err != nil {
			t.Fatalf("insert note_discovery_stats %s: %v", eventID, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_hashtags (event_id, author_pubkey, created_at, hashtag, derivation_version)
		VALUES
			('focal_evt', 'author_a', $1, 'nostr', 1),
			('hashtag_evt', 'author_b', $2, 'nostr', 1),
			('hashtag_evt', 'author_b', $2, 'bitcoin', 1),
			('thread_evt', 'author_c', $3, 'nostr', 1),
			('repost_evt', 'author_d', $4, 'nostr', 1)
	`, now.Add(-1*time.Hour).Unix(), now.Add(-2*time.Hour).Unix(), now.Add(-30*time.Minute).Unix(), now.Add(-20*time.Minute).Unix()); err != nil {
		t.Fatalf("insert event_hashtags: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO thread_edges (child_event_id, child_created_at, parent_event_id, root_event_id, parent_missing, root_missing, derivation_version)
		VALUES ('thread_evt', $1, 'focal_evt', 'focal_evt', FALSE, FALSE, 1)
	`, now.Add(-30*time.Minute).Unix()); err != nil {
		t.Fatalf("insert thread edge: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO repost_events (event_id, target_event_id, reposter_pubkey, quote, created_at, derivation_version)
		VALUES ('repost_evt', 'focal_evt', 'author_d', '', $1, 1)
	`, now.Add(-20*time.Minute).Unix()); err != nil {
		t.Fatalf("insert repost event: %v", err)
	}

	related, err := pgStore.GetRelatedNotes(ctx, "focal_evt", 3)
	if err != nil {
		t.Fatalf("GetRelatedNotes: %v", err)
	}
	if len(related) == 0 {
		t.Fatalf("expected related notes, got none")
	}
	if len(related) > 3 {
		t.Fatalf("expected bounded related notes size <= 3, got %d", len(related))
	}
	for _, row := range related {
		if row.EventID == "focal_evt" {
			t.Fatalf("focal event should not appear in related list")
		}
		if len(row.Reasons) == 0 {
			t.Fatalf("expected heuristic reason for related row: %#v", row)
		}
	}
}

func TestGetRelatedNotes_MissingReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)

	_, err := pgStore.GetRelatedNotes(ctx, "missing_evt", 10)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetNoteQuoteRepostLinkage_RollupAndRecentActivity(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC().Unix()

	mustInsertSimpleEventForNotePage(t, pool, "focal_evt", "author_f", 1, now-1000, "focal")
	mustInsertSimpleEventForNotePage(t, pool, "quote_evt", "author_q", 6, now-400, "quoted take")
	mustInsertSimpleEventForNotePage(t, pool, "repost_evt", "author_r", 6, now-300, "")

	if _, err := pool.Exec(ctx, `
		INSERT INTO repost_events (event_id, target_event_id, reposter_pubkey, quote, created_at, derivation_version)
		VALUES
			('quote_evt', 'focal_evt', 'author_q', 'quoted take', $1, 1),
			('repost_evt', 'focal_evt', 'author_r', '', $2, 1)
	`, now-400, now-300); err != nil {
		t.Fatalf("insert repost_events: %v", err)
	}

	linkage, err := pgStore.GetNoteQuoteRepostLinkage(ctx, "focal_evt", 10)
	if err != nil {
		t.Fatalf("GetNoteQuoteRepostLinkage: %v", err)
	}
	if linkage.QuoteCount != 1 || linkage.RepostCount != 1 {
		t.Fatalf("unexpected linkage counts: %#v", linkage)
	}
	if len(linkage.RecentActivity) != 2 {
		t.Fatalf("expected 2 recent linkage items, got %d", len(linkage.RecentActivity))
	}
	if linkage.RecentActivity[0].Action != "repost" || linkage.RecentActivity[1].Action != "quote" {
		t.Fatalf("unexpected linkage action ordering: %#v", linkage.RecentActivity)
	}
	if linkage.RecentActivity[1].LinkedNote.EventID != "quote_evt" {
		t.Fatalf("unexpected linked note summary: %#v", linkage.RecentActivity[1].LinkedNote)
	}
}

func newNotePageTestEvent(id, pubkey string, ts time.Time, hashtags []string) model.Event {
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
		"content":    "note page test",
		"sig":        "sig_" + id,
	})
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        1,
		Sig:         "sig_" + id,
		Content:     "note page test",
		RawJSON:     raw,
		FirstSeenAt: ts,
		InsertedAt:  ts,
	}
}

func mustInsertSimpleEventForNotePage(
	t *testing.T,
	pool *pgxpool.Pool,
	id string,
	pubkey string,
	kind int,
	createdAt int64,
	content string,
) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       [][]string{},
		"content":    content,
		"sig":        "sig_" + id,
	})
	if err != nil {
		t.Fatalf("marshal event raw json: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
	`, id, pubkey, createdAt, kind, "sig_"+id, content, string(raw)); err != nil {
		t.Fatalf("insert event %s: %v", id, err)
	}
}
