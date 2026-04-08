package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetPublicDiscoveryNetworkStats_ComputesWindowedCounts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC()

	type noteSeed struct {
		ID        string
		Pubkey    string
		CreatedAt time.Time
		Hashtag   string
	}
	notes := []noteSeed{
		{ID: "note_stats_1", Pubkey: "author_a", CreatedAt: now.Add(-2 * time.Hour), Hashtag: "nostr"},
		{ID: "note_stats_2", Pubkey: "author_b", CreatedAt: now.Add(-3 * time.Hour), Hashtag: "nostr"},
		{ID: "note_stats_3", Pubkey: "author_a", CreatedAt: now.Add(-30 * time.Hour), Hashtag: "bitcoin"},
		{ID: "note_stats_4", Pubkey: "author_c", CreatedAt: now.Add(-6 * 24 * time.Hour), Hashtag: "bitcoin"},
		{ID: "note_stats_5", Pubkey: "author_e", CreatedAt: now.Add(-4 * 24 * time.Hour), Hashtag: "nostr"},
		{ID: "note_stats_6", Pubkey: "author_d", CreatedAt: now.Add(-8 * 24 * time.Hour), Hashtag: "old"},
	}
	for _, note := range notes {
		event := newStoreStatsEvent(note.ID, note.Pubkey, 1, note.CreatedAt)
		tags := [][]string{{"t", note.Hashtag}}
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert note event %s: %v", note.ID, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO note_discovery_stats (event_id, author_pubkey, created_at, derivation_version)
			VALUES ($1, $2, $3, 1)
		`, note.ID, note.Pubkey, note.CreatedAt.Unix()); err != nil {
			t.Fatalf("insert note_discovery_stats %s: %v", note.ID, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO event_hashtags (event_id, author_pubkey, created_at, hashtag, derivation_version)
			VALUES ($1, $2, $3, $4, 1)
		`, note.ID, note.Pubkey, note.CreatedAt.Unix(), note.Hashtag); err != nil {
			t.Fatalf("insert event_hashtags %s: %v", note.ID, err)
		}
	}

	profileIDs := []string{"profile_meta_1", "profile_meta_2", "profile_meta_3"}
	for i, eventID := range profileIDs {
		pubkey := "profile_pubkey_" + string(rune('a'+i))
		event := newStoreStatsEvent(eventID, pubkey, 0, now.Add(-time.Duration(i+1)*time.Hour))
		if err := pgStore.InsertCanonicalEvent(ctx, event, nil, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert metadata event %s: %v", eventID, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO profiles_latest (
				pubkey, metadata_event_id, metadata_created_at, profile_json, derivation_version, name, display_name, about, nip05
			)
			VALUES ($1, $2, $3, '{}'::jsonb, 1, $4, $4, '', '')
		`, pubkey, eventID, event.CreatedAt, pubkey); err != nil {
			t.Fatalf("insert profile projection %s: %v", pubkey, err)
		}
	}

	stats, err := pgStore.GetPublicDiscoveryNetworkStats(ctx, 5)
	if err != nil {
		t.Fatalf("GetPublicDiscoveryNetworkStats: %v", err)
	}
	if stats.EventsIngested != 9 {
		t.Fatalf("unexpected events ingested: got=%d want=9", stats.EventsIngested)
	}
	if stats.ProjectedProfiles != 3 {
		t.Fatalf("unexpected projected profiles: got=%d want=3", stats.ProjectedProfiles)
	}
	if stats.ActiveAuthors.Last24h != 2 || stats.ActiveAuthors.Last7d != 4 {
		t.Fatalf("unexpected active authors windows: %#v", stats.ActiveAuthors)
	}
	if stats.NoteVolume.Last24h != 2 || stats.NoteVolume.Last7d != 5 {
		t.Fatalf("unexpected note volume windows: %#v", stats.NoteVolume)
	}
	if stats.TopHashtags == nil {
		t.Fatalf("expected top hashtags payload")
	}
	if len(stats.TopHashtags.Last24h) == 0 || stats.TopHashtags.Last24h[0].Hashtag != "nostr" {
		t.Fatalf("unexpected 24h top hashtags: %#v", stats.TopHashtags.Last24h)
	}
	if len(stats.TopHashtags.Last7d) == 0 || stats.TopHashtags.Last7d[0].Hashtag != "nostr" {
		t.Fatalf("unexpected 7d top hashtags: %#v", stats.TopHashtags.Last7d)
	}
}

func TestGetPublicDiscoveryNetworkStats_HandlesMissingHashtagsProjection(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pgStore := NewPostgresStore(pool)
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS event_hashtags`); err != nil {
		t.Fatalf("drop event_hashtags: %v", err)
	}

	stats, err := pgStore.GetPublicDiscoveryNetworkStats(ctx, 5)
	if err != nil {
		t.Fatalf("GetPublicDiscoveryNetworkStats with missing hashtags table: %v", err)
	}
	if stats.TopHashtags != nil {
		t.Fatalf("expected nil top hashtags when projection is unavailable, got %#v", stats.TopHashtags)
	}
	if stats.EventsIngested != 0 || stats.ProjectedProfiles != 0 || stats.NoteVolume.Last24h != 0 {
		t.Fatalf("unexpected empty stats payload: %#v", stats)
	}
}

func newStoreStatsEvent(id, pubkey string, kind int, ts time.Time) model.Event {
	createdAt := ts.Unix()
	raw, _ := json.Marshal(map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       [][]string{},
		"content":    "stats seed",
		"sig":        "sig_" + id,
	})
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        kind,
		Sig:         "sig_" + id,
		Content:     "stats seed",
		RawJSON:     raw,
		FirstSeenAt: ts,
		InsertedAt:  ts,
	}
}
