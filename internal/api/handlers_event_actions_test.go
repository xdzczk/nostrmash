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

func TestGetEventReplies_UsesCursorAndReturnsNextCursor(t *testing.T) {
	nextCursor := &store.EventOrderCursor{CreatedAt: 1001, ID: "evt_b"}
	encoded, err := encodeEventCursor(&query.EventCursor{CreatedAt: 1000, ID: "evt_a"})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	handlers := mustNewHandlers(t, fakeEventReader{
		getEventRepliesFn: func(_ context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			if eventID != "evt_parent" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			if limit != 2 {
				t.Fatalf("unexpected limit: %d", limit)
			}
			if cursor == nil || cursor.CreatedAt != 1000 || cursor.ID != "evt_a" {
				t.Fatalf("unexpected cursor: %#v", cursor)
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"evt_a"}`),
				json.RawMessage(`{"id":"evt_b"}`),
			}, nextCursor, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}/replies", handlers.GetEventReplies)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_parent/replies?limit=2&cursor="+encoded, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if strings.TrimSpace(resp.NextCursor) == "" {
		t.Fatalf("expected next_cursor in response")
	}
}
