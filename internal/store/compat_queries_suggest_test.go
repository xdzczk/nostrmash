package store

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestSuggestProfiles_MatchesPrefixAndContains(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newDiscoveryEvent("meta_alice", "pk_alice", now.Add(-2*time.Hour), 0, nil, `{"name":"alice","display_name":"Alice Nostr","nip05":"alice@nostr.example"}`),
		newDiscoveryEvent("meta_bob", "pk_bob", now.Add(-1*time.Hour), 0, nil, `{"name":"bob","display_name":"Builder","nip05":"builder@example.com"}`),
	}
	for _, event := range events {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectProfilesLatest(ctx, event.ID); err != nil {
			t.Fatalf("project profile %s: %v", event.ID, err)
		}
	}

	out, err := pgStore.SuggestProfiles(ctx, "ali", 5)
	if err != nil {
		t.Fatalf("SuggestProfiles: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("unexpected profile suggestion count: got=%d want=1", len(out))
	}
	if out[0].Pubkey != "pk_alice" {
		t.Fatalf("unexpected top profile suggestion: %#v", out[0])
	}
}

func TestSuggestHashtags_MatchesPrefixAndAggregates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newTaggedEvent("sg_hash_1", "author_a", now.Add(-30*time.Minute), "nostr"),
		newTaggedEvent("sg_hash_2", "author_b", now.Add(-25*time.Minute), "nostr"),
		newTaggedEvent("sg_hash_3", "author_b", now.Add(-20*time.Minute), "nostrich"),
		newTaggedEvent("sg_hash_4", "author_c", now.Add(-15*time.Minute), "bitcoin"),
	}
	for _, event := range events {
		tags := extractTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectEventHashtags(ctx, event.ID); err != nil {
			t.Fatalf("project hashtag %s: %v", event.ID, err)
		}
	}

	out, err := pgStore.SuggestHashtags(ctx, "nos", 5)
	if err != nil {
		t.Fatalf("SuggestHashtags: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("unexpected hashtag suggestion count: got=%d want=2", len(out))
	}
	if out[0].Hashtag != "nostr" || out[0].EventCount != 2 {
		t.Fatalf("unexpected top hashtag suggestion: %#v", out[0])
	}
	if out[1].Hashtag != "nostrich" {
		t.Fatalf("unexpected second hashtag suggestion: %#v", out[1])
	}
}
