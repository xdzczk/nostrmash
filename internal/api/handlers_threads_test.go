package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetThread_UsesSharedServiceAndPreservesResponseShape(t *testing.T) {
	cursor, err := encodeEventCursor(&query.EventCursor{CreatedAt: 1000, ID: "evt_cursor"})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	next := &store.EventOrderCursor{CreatedAt: 999, ID: "evt_next"}

	handlers := NewHandlers(fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, eventID string) (json.RawMessage, error) {
			if eventID != "evt_parent" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			return json.RawMessage(`{"id":"evt_parent"}`), nil
		},
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			if eventID != "evt_parent" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			if maxDepth != 4 {
				t.Fatalf("unexpected max depth: %d", maxDepth)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_root"}`)}, []string{"evt_missing"}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			if eventID != "evt_parent" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			if limit != 2 {
				t.Fatalf("unexpected limit: %d", limit)
			}
			if cursor == nil || cursor.CreatedAt != 1000 || cursor.ID != "evt_cursor" {
				t.Fatalf("unexpected cursor: %#v", cursor)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_reply_1"}`)}, next, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/threads/{eventId}", handlers.GetThread)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/evt_parent?limit=2&max_depth=4&cursor="+cursor, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		EventID          string            `json:"event_id"`
		Event            json.RawMessage   `json:"event"`
		Ancestors        []json.RawMessage `json:"ancestors"`
		MissingAncestors []string          `json:"missing_ancestor_ids"`
		Replies          []json.RawMessage `json:"replies"`
		NextCursor       string            `json:"next_cursor"`
		Consistency      string            `json:"consistency"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.EventID != "evt_parent" || len(resp.Ancestors) != 1 || len(resp.Replies) != 1 || resp.Consistency != "eventual" {
		t.Fatalf("unexpected thread response: %+v", resp)
	}
	if len(resp.MissingAncestors) != 1 || resp.MissingAncestors[0] != "evt_missing" {
		t.Fatalf("unexpected missing ancestors: %#v", resp.MissingAncestors)
	}
	if strings.TrimSpace(resp.NextCursor) == "" {
		t.Fatal("expected next_cursor to be present")
	}
}
