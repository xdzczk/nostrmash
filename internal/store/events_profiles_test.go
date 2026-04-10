package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
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
