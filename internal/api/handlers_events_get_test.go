package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetEventByID_NotFoundUsesErrorEnvelopeAndRequestID(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventWithProvFn: func(_ context.Context, _ string) (store.EventWithProvenance, error) {
			return store.EventWithProvenance{}, store.ErrNotFound
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}", handlers.GetEventByID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/missing", nil)
	req.Header.Set("X-Request-ID", "req-test-123")
	rec := httptest.NewRecorder()
	WithRequestID(mux).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNotFound)
	}
	if got := rec.Header().Get("X-Request-ID"); got != "req-test-123" {
		t.Fatalf("unexpected response request id: got %q", got)
	}

	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.Error.Code != "not_found" {
		t.Fatalf("unexpected error code: got %q", envelope.Error.Code)
	}
	if envelope.Error.RequestID != "req-test-123" {
		t.Fatalf("unexpected request id in envelope: got %q", envelope.Error.RequestID)
	}
}

func TestGetEventByID_ReturnsEventAndProvenance(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventWithProvFn: func(_ context.Context, _ string) (store.EventWithProvenance, error) {
			return store.EventWithProvenance{
				Event: json.RawMessage(`{"id":"evt_1","kind":1}`),
				Relays: []model.EventRelay{
					{RelayURL: "wss://relay.one", SeenAt: time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC)},
				},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}", handlers.GetEventByID)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Event map[string]any `json:"event"`
		Prov  struct {
			Relays []seenOnEntry `json:"relays"`
		} `json:"provenance"`
		Consistency string `json:"consistency"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Event["id"] != "evt_1" || len(resp.Prov.Relays) != 1 || resp.Consistency != "strong" {
		t.Fatalf("unexpected response payload: %+v", resp)
	}
}
