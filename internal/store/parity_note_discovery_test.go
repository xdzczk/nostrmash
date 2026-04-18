package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetTrendingNotes_WindowsAndOrdering(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newDiscoveryEvent("meta_a", "author_a", now.Add(-5*time.Hour), 0, nil, `{"name":"Author A"}`),
		newDiscoveryEvent("meta_b", "author_b", now.Add(-5*time.Hour), 0, nil, `{"name":"Author B"}`),
		newDiscoveryEvent("meta_c", "author_c", now.Add(-50*time.Hour), 0, nil, `{"name":"Author C"}`),
		newDiscoveryEvent("note_a", "author_a", now.Add(-2*time.Hour), 1, nil, "note a"),
		newDiscoveryEvent("note_b", "author_b", now.Add(-3*time.Hour), 1, nil, "note b"),
		newDiscoveryEvent("note_c", "author_c", now.Add(-40*time.Hour), 1, nil, "note c"),
		newDiscoveryEvent("react_a1", "reactor_1", now.Add(-90*time.Minute), 7, [][]string{{"e", "note_a"}}, "+"),
		newDiscoveryEvent("react_a2", "reactor_2", now.Add(-80*time.Minute), 7, [][]string{{"e", "note_a"}}, "+"),
		newDiscoveryEvent("reply_a1", "replier_1", now.Add(-70*time.Minute), 1, [][]string{{"e", "note_a", "", "reply"}}, "reply"),
		newDiscoveryEvent("react_b1", "reactor_3", now.Add(-30*time.Minute), 7, [][]string{{"e", "note_b"}}, "+"),
		newDiscoveryEvent("reply_c1", "replier_2", now.Add(-20*time.Hour), 1, [][]string{{"e", "note_c", "", "reply"}}, "reply"),
		newDiscoveryEvent("reply_c2", "replier_3", now.Add(-10*time.Hour), 1, [][]string{{"e", "note_c", "", "reply"}}, "reply"),
		newDiscoveryEvent("react_c1", "reactor_4", now.Add(-8*time.Hour), 7, [][]string{{"e", "note_c"}}, "+"),
	}
	for _, event := range events {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	last24h, err := pgStore.GetTrendingNotes(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingNotes 24h: %v", err)
	}
	if len(last24h) != 2 {
		t.Fatalf("unexpected 24h note count: got=%d want=2", len(last24h))
	}
	if last24h[0].EventID != "note_a" {
		t.Fatalf("expected note_a to rank above note_b in 24h window, got %#v", last24h[0])
	}
	if last24h[1].EventID != "note_b" {
		t.Fatalf("expected note_b second in 24h window, got %#v", last24h[1])
	}

	last7d, err := pgStore.GetTrendingNotes(ctx, 7*24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingNotes 7d: %v", err)
	}
	if len(last7d) != 3 {
		t.Fatalf("unexpected 7d note count: got=%d want=3", len(last7d))
	}
	if last7d[0].EventID != "note_c" {
		t.Fatalf("expected older but highly engaged note_c to rank first in 7d window, got %#v", last7d[0])
	}
}

func TestGetTrendingNotes_ExcludesNotesFromAuthorsWithoutLocalMetadata(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	pgStore := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newDiscoveryEvent("meta_resolved", "resolved_author", now.Add(-4*time.Hour), 0, nil, `{"name":"Resolved"}`),
		newDiscoveryEvent("resolved_note", "resolved_author", now.Add(-2*time.Hour), 1, nil, "resolved note"),
		newDiscoveryEvent("resolved_react_1", "resolved_reactor_1", now.Add(-90*time.Minute), 7, [][]string{{"e", "resolved_note"}}, "+"),
		newDiscoveryEvent("resolved_react_2", "resolved_reactor_2", now.Add(-80*time.Minute), 7, [][]string{{"e", "resolved_note"}}, "+"),
		newDiscoveryEvent("unresolved_note", "unresolved_author", now.Add(-70*time.Minute), 1, nil, "unresolved note"),
		newDiscoveryEvent("unresolved_react_1", "unresolved_reactor_1", now.Add(-60*time.Minute), 7, [][]string{{"e", "unresolved_note"}}, "+"),
		newDiscoveryEvent("unresolved_react_2", "unresolved_reactor_2", now.Add(-50*time.Minute), 7, [][]string{{"e", "unresolved_note"}}, "+"),
		newDiscoveryEvent("unresolved_react_3", "unresolved_reactor_3", now.Add(-40*time.Minute), 7, [][]string{{"e", "unresolved_note"}}, "+"),
	}
	for _, event := range events {
		tags := extractDiscoveryTagsForStoreTest(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	notes, err := pgStore.GetTrendingNotes(ctx, 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingNotes: %v", err)
	}
	if len(notes) != 1 {
		t.Fatalf("expected only notes from resolved authors, got %d notes: %#v", len(notes), notes)
	}
	if notes[0].EventID != "resolved_note" {
		t.Fatalf("expected resolved_note only, got %#v", notes)
	}
}

func newDiscoveryEvent(id, pubkey string, ts time.Time, kind int, tags [][]string, content string) model.Event {
	createdAt := ts.Unix()
	raw, _ := json.Marshal(map[string]any{
		"id":         id,
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       tags,
		"content":    content,
		"sig":        "sig_" + id,
	})
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        kind,
		Sig:         "sig_" + id,
		Content:     content,
		RawJSON:     raw,
		FirstSeenAt: ts,
		InsertedAt:  ts,
	}
}

func extractDiscoveryTagsForStoreTest(t *testing.T, raw json.RawMessage) [][]string {
	t.Helper()
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode tags from raw event: %v", err)
	}
	return payload.Tags
}

// drainPendingProfileStatsForStoreTest synchronously runs the
// profile-stats sweeper until the dirty queue is empty so store-package
// tests can assert on profile_public_stats / profile_discovery_stats
// rows immediately after their DeriveEventBundle fan-out (in production
// these projections run out-of-band on a background sweeper).
func drainPendingProfileStatsForStoreTest(t *testing.T, ctx context.Context, handlers *derivation.Handlers) {
	t.Helper()
	for safety := 0; safety < 64; safety++ {
		processed, err := handlers.DrainPendingProfileStatsBatch(ctx, 64)
		if err != nil {
			t.Fatalf("drain pending profile stats: %v", err)
		}
		if processed == 0 {
			return
		}
	}
	t.Fatalf("drain pending profile stats did not converge after 64 batches")
}
