package query

import (
	"encoding/json"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestWindowDescendingRepliesReversesAscendingBase(t *testing.T) {
	t.Parallel()
	base := []json.RawMessage{
		json.RawMessage(`{"id":"a","created_at":1}`),
		json.RawMessage(`{"id":"b","created_at":2}`),
	}
	got, next := WindowDescendingReplies(base, nil, 10, nil, 0)
	if len(got) != 2 {
		t.Fatalf("len=%d", len(got))
	}
	var ids [2]string
	for i, raw := range got {
		var p struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
		ids[i] = p.ID
	}
	if ids[0] != "b" || ids[1] != "a" {
		t.Fatalf("expected descending b,a got %v", ids)
	}
	if next != nil {
		t.Fatalf("expected nil next, got %#v", next)
	}
}

func TestWindowDescendingRepliesOffsetAndLimit(t *testing.T) {
	t.Parallel()
	base := []json.RawMessage{
		json.RawMessage(`{"id":"r1","created_at":1}`),
		json.RawMessage(`{"id":"r2","created_at":2}`),
		json.RawMessage(`{"id":"r3","created_at":3}`),
	}
	got, next := WindowDescendingReplies(base, nil, 1, nil, 1)
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	var p struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(got[0], &p); err != nil {
		t.Fatal(err)
	}
	if p.ID != "r2" {
		t.Fatalf("expected r2, got %q", p.ID)
	}
	if next == nil || next.ID != "r2" {
		t.Fatalf("unexpected next: %#v", next)
	}
}

func TestWindowDescendingRepliesCursorSkipsThroughWindow(t *testing.T) {
	t.Parallel()
	base := []json.RawMessage{
		json.RawMessage(`{"id":"r1","created_at":1}`),
		json.RawMessage(`{"id":"r2","created_at":2}`),
		json.RawMessage(`{"id":"r3","created_at":3}`),
	}
	cur := &store.EventOrderCursor{CreatedAt: 3, ID: "r3"}
	got, next := WindowDescendingReplies(base, nil, 1, cur, 0)
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	var p struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(got[0], &p); err != nil {
		t.Fatal(err)
	}
	if p.ID != "r2" {
		t.Fatalf("expected r2 after cursor, got %q", p.ID)
	}
	if next == nil || next.ID != "r2" {
		t.Fatalf("expected next r2, got %#v", next)
	}
}

func TestWindowDescendingRepliesMergesExtraDedupesAndSorts(t *testing.T) {
	t.Parallel()
	base := []json.RawMessage{
		json.RawMessage(`{"id":"r2","created_at":2}`),
		json.RawMessage(`{"id":"r1","created_at":1}`),
	}
	extra := []json.RawMessage{
		json.RawMessage(`{"id":"r2","created_at":99}`),
		json.RawMessage(`{"id":"r3","created_at":3}`),
	}
	got, _ := WindowDescendingReplies(base, extra, 10, nil, 0)
	if len(got) != 3 {
		t.Fatalf("len=%d", len(got))
	}
	var ids []string
	for _, raw := range got {
		var p struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, p.ID)
	}
	if ids[0] != "r3" || ids[1] != "r2" || ids[2] != "r1" {
		t.Fatalf("unexpected order %v", ids)
	}
}

func TestWindowDescendingRepliesSkipsMalformedAndEmptyID(t *testing.T) {
	t.Parallel()
	base := []json.RawMessage{
		json.RawMessage(`not-json`),
		json.RawMessage(`{"id":"","created_at":1}`),
		json.RawMessage(`{"id":"ok","created_at":2}`),
	}
	got, _ := WindowDescendingReplies(base, nil, 10, nil, 0)
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	var p struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(got[0], &p); err != nil {
		t.Fatal(err)
	}
	if p.ID != "ok" {
		t.Fatalf("got %q", p.ID)
	}
}
