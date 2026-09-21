package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetAuthorEvents_EmitsNextCursorAndResumesFromIt(t *testing.T) {
	var secondCallCursor *store.EventOrderCursor
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorEventsPageFn: func(_ context.Context, pubkey string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			if pubkey != "pubkey_x" || limit != 2 {
				t.Fatalf("unexpected args: pubkey=%s limit=%d", pubkey, limit)
			}
			if cursor == nil {
				return []json.RawMessage{
					json.RawMessage(`{"id":"evt_new","created_at":300}`),
					json.RawMessage(`{"id":"evt_mid","created_at":200}`),
				}, &store.EventOrderCursor{CreatedAt: 200, ID: "evt_mid"}, nil
			}
			secondCallCursor = cursor
			return []json.RawMessage{json.RawMessage(`{"id":"evt_old","created_at":100}`)}, nil, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/events", handlers.GetAuthorEvents)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/events?limit=2", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var firstPage struct {
		Events     []json.RawMessage `json:"events"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(firstPage.Events) != 2 {
		t.Fatalf("unexpected first page size: %d", len(firstPage.Events))
	}
	if firstPage.NextCursor == "" {
		t.Fatalf("expected next_cursor on first page")
	}

	req2 := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/authors/pubkey_x/events?limit=2&cursor="+url.QueryEscape(firstPage.NextCursor),
		nil,
	)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("unexpected continuation status: got %d want %d", rec2.Code, http.StatusOK)
	}
	if secondCallCursor == nil || secondCallCursor.CreatedAt != 200 || secondCallCursor.ID != "evt_mid" {
		t.Fatalf("unexpected decoded resume cursor: %+v", secondCallCursor)
	}
	var secondPage struct {
		Events     []json.RawMessage `json:"events"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.NewDecoder(rec2.Body).Decode(&secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(secondPage.Events) != 1 || secondPage.NextCursor != "" {
		t.Fatalf("unexpected final page: %+v", secondPage)
	}
}

func TestGetAuthorEvents_KindFilterUsesKindPagedReader(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorEventsByKindPageFn: func(_ context.Context, pubkey string, kind int, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			if pubkey != "pubkey_x" || kind != 1 || limit != 20 || cursor != nil {
				t.Fatalf("unexpected args: pubkey=%s kind=%d limit=%d cursor=%+v", pubkey, kind, limit, cursor)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"note_1","kind":1}`)}, nil, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/events", handlers.GetAuthorEvents)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/events?kind=1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var payload struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Events) != 1 {
		t.Fatalf("unexpected event count: %d", len(payload.Events))
	}
}

func TestGetAuthorEvents_RejectsMalformedCursor(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/events", handlers.GetAuthorEvents)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/events?cursor=%21%21not-base64%21%21", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
	}
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if payload.Error.Code != "invalid_cursor" {
		t.Fatalf("unexpected error code: %q", payload.Error.Code)
	}
}
