package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func insertEventWithTags(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, createdAt int64, tags [][]string) {
	t.Helper()
	insertEventWithKindAndTags(t, ctx, pool, id, 1, createdAt, tags)
}

func insertEventWithKindAndTags(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, kind int, createdAt int64, tags [][]string) {
	t.Helper()
	raw := fmt.Sprintf(`{"id":%q}`, id)
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, 'pub', $2, $3, 'sig', '', $4::jsonb)
	`, id, createdAt, kind, raw); err != nil {
		t.Fatalf("insert event %s: %v", id, err)
	}
	for tagIndex, tag := range tags {
		for valueIndex, value := range tag[1:] {
			if _, err := pool.Exec(ctx, `
				INSERT INTO event_tags (event_id, tag_name, tag_index, value_index, value)
				VALUES ($1, $2, $3, $4, $5)
			`, id, tag[0], tagIndex, valueIndex, value); err != nil {
				t.Fatalf("insert tag for %s: %v", id, err)
			}
		}
	}
}

// TestGetEventsReferencingPubkey covers the event_tags-backed mentions read
// that replaced the dropped pubkey_references projection (migration 000053).
func TestGetEventsReferencingPubkey(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	const target = "target_pub"

	// Newest first in expected output.
	insertEventWithTags(t, ctx, pool, "m_new", 3000, [][]string{{"p", target}})
	// Two p-tags to the same target must yield the event once.
	insertEventWithTags(t, ctx, pool, "m_double", 2000, [][]string{{"p", target}, {"p", target}})
	// p-tag with a relay hint at value_index 1: hint must not match as a target.
	insertEventWithTags(t, ctx, pool, "m_hinted", 1000, [][]string{{"p", target, "wss://relay.example"}})
	// Non-matches: different pubkey, an e-tag whose value equals the target,
	// and a kind-3 contact list that follows the target (follow ≠ mention).
	insertEventWithTags(t, ctx, pool, "x_other_pub", 2500, [][]string{{"p", "other_pub"}})
	insertEventWithTags(t, ctx, pool, "x_etag", 2600, [][]string{{"e", target}})
	insertEventWithKindAndTags(t, ctx, pool, "x_contact", 3, 2700, [][]string{{"p", target}})

	got, err := NewPostgresStore(pool).GetEventsReferencingPubkey(ctx, target, 10)
	if err != nil {
		t.Fatalf("get events referencing pubkey: %v", err)
	}

	wantIDs := []string{"m_new", "m_double", "m_hinted"}
	if len(got) != len(wantIDs) {
		t.Fatalf("expected %d events, got %d: %s", len(wantIDs), len(got), got)
	}
	for i, raw := range got {
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("unmarshal result %d: %v", i, err)
		}
		if payload.ID != wantIDs[i] {
			t.Fatalf("result %d: got %q want %q", i, payload.ID, wantIDs[i])
		}
	}

	// Limit applies after dedup.
	limited, err := NewPostgresStore(pool).GetEventsReferencingPubkey(ctx, target, 2)
	if err != nil {
		t.Fatalf("get limited: %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("expected 2 limited events, got %d", len(limited))
	}
}
