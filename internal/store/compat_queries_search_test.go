package store

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestSearchProfiles_PrefersLatestMetadataWhenProjectionIsStale(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	now := time.Now().UTC()
	oldEvent := newDiscoveryEvent(
		"meta_search_old",
		"pk_search_stale",
		now.Add(-2*time.Hour),
		0,
		nil,
		`{"name":"old-handle","display_name":"Old Name"}`,
	)
	newEvent := newDiscoveryEvent(
		"meta_search_new",
		"pk_search_stale",
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
	`, "pk_search_stale", oldEvent.ID, oldEvent.CreatedAt, `{"name":"old-handle","display_name":"Old Name"}`, "old-handle", "Old Name"); err != nil {
		t.Fatalf("seed stale projection: %v", err)
	}

	rows, err := pgStore.SearchProfilesWithOptions(ctx, "fiatjaf", "relevant", 5, 0)
	if err != nil {
		t.Fatalf("SearchProfilesWithOptions: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one fresh profile match, got %#v", rows)
	}
	if rows[0].MetadataEventID != newEvent.ID {
		t.Fatalf("expected search to prefer latest metadata event, got %#v", rows[0])
	}
}
