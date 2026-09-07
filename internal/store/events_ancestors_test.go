package store

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestGetEventAncestors_OrdersRootToParent(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	baseTime := time.Date(2026, 4, 4, 16, 0, 0, 0, time.UTC)

	root := model.Event{
		ID:          "thread_root",
		Pubkey:      "author_a",
		CreatedAt:   1000,
		Kind:        1,
		Sig:         "sig_root",
		Content:     "root",
		RawJSON:     json.RawMessage(`{"id":"thread_root","kind":1,"tags":[]}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	parentTags := [][]string{{"e", "thread_root", "", "reply"}, {"e", "thread_root", "", "root"}}
	parent := model.Event{
		ID:          "thread_parent",
		Pubkey:      "author_b",
		CreatedAt:   1001,
		Kind:        1,
		Sig:         "sig_parent",
		Content:     "parent",
		RawJSON:     json.RawMessage(`{"id":"thread_parent","kind":1,"tags":[["e","thread_root","","reply"],["e","thread_root","","root"]]}`),
		FirstSeenAt: baseTime.Add(1 * time.Second),
		InsertedAt:  baseTime.Add(1 * time.Second),
	}
	childTags := [][]string{{"e", "thread_root", "", "root"}, {"e", "thread_parent", "", "reply"}}
	child := model.Event{
		ID:          "thread_child",
		Pubkey:      "author_c",
		CreatedAt:   1002,
		Kind:        1,
		Sig:         "sig_child",
		Content:     "child",
		RawJSON:     json.RawMessage(`{"id":"thread_child","kind":1,"tags":[["e","thread_root","","root"],["e","thread_parent","","reply"]]}`),
		FirstSeenAt: baseTime.Add(2 * time.Second),
		InsertedAt:  baseTime.Add(2 * time.Second),
	}

	for _, event := range []struct {
		event model.Event
		tags  [][]string
	}{
		{root, nil},
		{parent, parentTags},
		{child, childTags},
	} {
		if err := s.InsertCanonicalEvent(ctx, event.event, event.tags, "wss://relay.one", event.event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.event.ID, err)
		}
	}
	if err := handlers.UpdateThreadProjection(ctx, parent.ID); err != nil {
		t.Fatalf("project parent thread edge: %v", err)
	}
	if err := handlers.UpdateThreadProjection(ctx, child.ID); err != nil {
		t.Fatalf("project child thread edge: %v", err)
	}

	ancestors, missing, err := s.GetEventAncestors(ctx, child.ID, 10)
	if err != nil {
		t.Fatalf("get event ancestors: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing ancestors, got %#v", missing)
	}
	if len(ancestors) != 2 {
		t.Fatalf("expected 2 ancestors, got %d", len(ancestors))
	}
	ids := decodeEventIDs(t, ancestors)
	want := []string{"thread_root", "thread_parent"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("unexpected ancestor ordering: got=%v want=%v", ids, want)
	}

	// maxDepth=1 must stop at the immediate parent (one CTE hop).
	shallow, _, err := s.GetEventAncestors(ctx, child.ID, 1)
	if err != nil {
		t.Fatalf("shallow ancestors: %v", err)
	}
	if got := decodeEventIDs(t, shallow); !reflect.DeepEqual(got, []string{"thread_parent"}) {
		t.Fatalf("maxDepth=1: got=%v want=[thread_parent]", got)
	}
}

func TestGetEventAncestors_MissingParentAndCycle(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 16, 0, 0, 0, time.UTC)

	child := model.Event{
		ID:          "orphan_child",
		Pubkey:      "author_c",
		CreatedAt:   1002,
		Kind:        1,
		Sig:         "sig_child",
		Content:     "child",
		RawJSON:     json.RawMessage(`{"id":"orphan_child","kind":1}`),
		FirstSeenAt: baseTime,
		InsertedAt:  baseTime,
	}
	if err := s.InsertCanonicalEvent(ctx, child, nil, "wss://relay.one", child.FirstSeenAt); err != nil {
		t.Fatalf("insert child: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO thread_edges (
			child_event_id, child_created_at, parent_event_id, root_event_id,
			parent_missing, root_missing, derivation_version
		) VALUES
			('orphan_child', 1002, 'missing_parent', 'missing_parent', true, true, 1)
	`); err != nil {
		t.Fatalf("insert missing-parent edge: %v", err)
	}

	ancestors, missing, err := s.GetEventAncestors(ctx, child.ID, 10)
	if err != nil {
		t.Fatalf("get ancestors: %v", err)
	}
	if len(ancestors) != 0 {
		t.Fatalf("expected no resolved ancestors, got %d", len(ancestors))
	}
	if !reflect.DeepEqual(missing, []string{"missing_parent"}) {
		t.Fatalf("missing ids: got=%v want=[missing_parent]", missing)
	}

	// A cycle must not loop forever: A → B → A.
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES
			('cycle_a', 'pk', 1, 1, 's', '', '{}'),
			('cycle_b', 'pk', 2, 1, 's', '', '{}');
		INSERT INTO thread_edges (
			child_event_id, child_created_at, parent_event_id, root_event_id,
			parent_missing, root_missing, derivation_version
		) VALUES
			('cycle_a', 1, 'cycle_b', 'cycle_b', false, false, 1),
			('cycle_b', 2, 'cycle_a', 'cycle_a', false, false, 1)
	`); err != nil {
		t.Fatalf("insert cycle: %v", err)
	}
	cycled, cycledMissing, err := s.GetEventAncestors(ctx, "cycle_a", 50)
	if err != nil {
		t.Fatalf("cycle walk: %v", err)
	}
	if len(cycled)+len(cycledMissing) > 2 {
		t.Fatalf("cycle walk escaped: ancestors=%d missing=%d", len(cycled), len(cycledMissing))
	}
}
