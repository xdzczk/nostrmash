package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestProfileReads_FallBackToLatestMetadataEventsWhenProjectionMissing(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC()
	events := []struct {
		id      string
		pubkey  string
		content string
		at      time.Time
	}{
		{
			id:      "meta_old_a",
			pubkey:  "pk_a",
			content: `{"name":"alice-old","display_name":"Alice Old"}`,
			at:      now.Add(-2 * time.Hour),
		},
		{
			id:      "meta_new_a",
			pubkey:  "pk_a",
			content: `{"name":"alice","display_name":"Alice","picture":"https://cdn.example/alice.png"}`,
			at:      now.Add(-1 * time.Hour),
		},
		{
			id:      "meta_b",
			pubkey:  "pk_b",
			content: `{"name":"bob","display_name":"Bob"}`,
			at:      now.Add(-90 * time.Minute),
		},
	}
	for _, seed := range events {
		event := newDiscoveryEvent(seed.id, seed.pubkey, seed.at, 0, nil, seed.content)
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}

	profile, err := pgStore.GetProfileByPubkey(ctx, "pk_a")
	if err != nil {
		t.Fatalf("GetProfileByPubkey: %v", err)
	}
	if profile.MetadataEventID != "meta_new_a" {
		t.Fatalf("expected latest metadata event to win, got %#v", profile)
	}
	var decoded map[string]any
	if err := json.Unmarshal(profile.ProfileJSON, &decoded); err != nil {
		t.Fatalf("decode profile json: %v", err)
	}
	if decoded["display_name"] != "Alice" || decoded["picture"] != "https://cdn.example/alice.png" {
		t.Fatalf("expected hydrated profile json from latest metadata event, got %#v", decoded)
	}

	batch, err := pgStore.GetProfilesByPubkeys(ctx, []string{"pk_a", "pk_b", "missing"})
	if err != nil {
		t.Fatalf("GetProfilesByPubkeys: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected two locally recoverable profiles, got %#v", batch)
	}
	if _, ok := batch["pk_a"]; !ok {
		t.Fatalf("expected pk_a in batch result, got %#v", batch)
	}
	if _, ok := batch["pk_b"]; !ok {
		t.Fatalf("expected pk_b in batch result, got %#v", batch)
	}
	if _, ok := batch["missing"]; ok {
		t.Fatalf("did not expect missing pubkey in batch result, got %#v", batch["missing"])
	}
}

func TestProfileReads_PreferLatestMetadataEventsWhenProjectionIsStale(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC()
	oldEvent := newDiscoveryEvent(
		"meta_stale_old",
		"pk_stale",
		now.Add(-2*time.Hour),
		0,
		nil,
		`{"name":"old-handle","display_name":"Old Name"}`,
	)
	newEvent := newDiscoveryEvent(
		"meta_stale_new",
		"pk_stale",
		now.Add(-1*time.Hour),
		0,
		nil,
		`{"name":"fiatjaf","display_name":"fiatjaf"}`,
	)
	for _, event := range []model.Event{oldEvent, newEvent} {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO profiles_latest (
			pubkey, metadata_event_id, metadata_created_at, profile_json, name, display_name, about, nip05, derivation_version
		)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, '', '', 1)
	`, "pk_stale", oldEvent.ID, oldEvent.CreatedAt, `{"name":"old-handle","display_name":"Old Name"}`, "old-handle", "Old Name"); err != nil {
		t.Fatalf("seed stale projection: %v", err)
	}

	profile, err := pgStore.GetProfileByPubkey(ctx, "pk_stale")
	if err != nil {
		t.Fatalf("GetProfileByPubkey: %v", err)
	}
	if profile.MetadataEventID != newEvent.ID {
		t.Fatalf("expected latest metadata event to override stale projection, got %#v", profile)
	}

	batch, err := pgStore.GetProfilesByPubkeys(ctx, []string{"pk_stale"})
	if err != nil {
		t.Fatalf("GetProfilesByPubkeys: %v", err)
	}
	if batch["pk_stale"].MetadataEventID != newEvent.ID {
		t.Fatalf("expected batch lookup to override stale projection, got %#v", batch["pk_stale"])
	}
}
