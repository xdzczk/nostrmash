package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetThread_NotFoundWhenFocalEventMissing(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
		getEventAncestors: func(_ context.Context, _ string, _ int) ([]json.RawMessage, []string, error) {
			t.Fatal("expected no ancestor lookup when root event is missing")
			return nil, nil, nil
		},
		getEventRepliesFn: func(_ context.Context, _ string, _ int, _ *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			t.Fatal("expected no reply lookup when root event is missing")
			return nil, nil, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/threads/{eventId}", handlers.GetThread)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/missing", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNotFound)
	}
}

func TestGetThread_AncestorNotFoundStillInternalError(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, _ string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"evt_parent"}`), nil
		},
		getEventAncestors: func(_ context.Context, _ string, _ int) ([]json.RawMessage, []string, error) {
			return nil, nil, store.ErrNotFound
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/threads/{eventId}", handlers.GetThread)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/evt_parent", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestGetThreadSummary_NotFoundWhenRootMissing(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getThreadSummaryFn: func(_ context.Context, _ string) (store.ThreadSummaryProjection, error) {
			return store.ThreadSummaryProjection{}, store.ErrNotFound
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/threads/{root_event_id}/summary", handlers.GetThreadSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/missing/summary", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNotFound)
	}
}

func TestGetThreadSummary_ReturnsNotImplementedWithoutCapability(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getThreadSummaryFn: func(_ context.Context, _ string) (store.ThreadSummaryProjection, error) {
			return store.ThreadSummaryProjection{}, query.ErrUnsupportedCapability
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/threads/{root_event_id}/summary", handlers.GetThreadSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/root/summary", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNotImplemented)
	}
}
