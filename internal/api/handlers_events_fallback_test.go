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

func TestGetEventByID_LocalMissRelayFallbackSuccess(t *testing.T) {
	handlers := mustNewHandlersWithOptions(t, fakeEventReader{
		getEventWithProvFn: func(context.Context, string) (store.EventWithProvenance, error) {
			return store.EventWithProvenance{}, store.ErrNotFound
		},
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, HandlersOptions{
		MaxBatchSize: 10,
		QueryOptions: query.ServiceOptions{
			FallbackReader: apiFakeFallbackReader{
				fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
					return map[string]json.RawMessage{
						"evt_1": json.RawMessage(`{"id":"evt_1","kind":1}`),
					}, nil
				},
			},
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}", handlers.GetEventByID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var payload struct {
		Consistency string `json:"consistency"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Consistency != "eventual" {
		t.Fatalf("unexpected consistency for fallback path: %q", payload.Consistency)
	}
}

type apiFakeFallbackReader struct {
	fetchEventsByIDsFn       func(context.Context, []string) (map[string]json.RawMessage, error)
	fetchProfilesByPubkeysFn func(context.Context, []string) (map[string]store.ProfileProjection, error)
}

func (f apiFakeFallbackReader) FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.fetchEventsByIDsFn == nil {
		return map[string]json.RawMessage{}, nil
	}
	return f.fetchEventsByIDsFn(ctx, ids)
}

func (f apiFakeFallbackReader) FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
	if f.fetchProfilesByPubkeysFn == nil {
		return map[string]store.ProfileProjection{}, nil
	}
	return f.fetchProfilesByPubkeysFn(ctx, pubkeys)
}
