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
}
