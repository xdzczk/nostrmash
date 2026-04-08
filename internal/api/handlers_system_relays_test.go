package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestReady_UsesErrorEnvelopeWhenDependencyUnavailable(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	req.Header.Set("X-Request-ID", "req-ready-1")
	rec := httptest.NewRecorder()
	handler := WithRequestID(Ready(nil))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusServiceUnavailable)
	}
	var resp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Error.Code != "dependency_unavailable" {
		t.Fatalf("unexpected error code: %s", resp.Error.Code)
	}
}

func TestGetRelaysHealth_ReturnsPersistedCheckpointRows(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		listRelayHealthFn: func(_ context.Context) ([]model.IngestCheckpoint, error) {
			return []model.IngestCheckpoint{
				{
					RelayURL:    "wss://relay.one",
					Mode:        "live",
					FilterGroup: "social_core",
					Status:      "healthy",
					UpdatedAt:   time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC),
				},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/relays/health", handlers.GetRelaysHealth)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/relays/health", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Relays []relayHealthEntry `json:"relays"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Relays) != 1 || resp.Relays[0].RelayURL != "wss://relay.one" {
		t.Fatalf("unexpected relays payload: %+v", resp.Relays)
	}
}
