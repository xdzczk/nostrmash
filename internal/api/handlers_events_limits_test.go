package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestBatchGetEvents_EnforcesConfiguredLimit(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{}, 2)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", strings.NewReader(`{"ids":["a","b","c"]}`))
	rec := httptest.NewRecorder()
	WithRequestID(http.HandlerFunc(handlers.BatchGetEvents)).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
	}

	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.Error.Code != "batch_limit_exceeded" {
		t.Fatalf("unexpected error code: got %q", envelope.Error.Code)
	}
	if envelope.Error.RequestID == "" {
		t.Fatal("expected generated request id in error envelope")
	}
}

func TestGetEventSeenOn_Success(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventSeenOnByID: func(_ context.Context, id string) ([]model.EventRelay, error) {
			return []model.EventRelay{
				{EventID: id, RelayURL: "wss://relay.one", SeenAt: time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC)},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}/seen-on", handlers.GetEventSeenOn)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_1/seen-on", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
}
