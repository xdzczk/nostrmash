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
	lastErr := "dial websocket: websocket: bad handshake"
	healthyAt := time.Now().UTC().Add(-time.Minute)
	erroredAt := time.Now().UTC()
	staleAt := time.Now().UTC().Add(-48 * time.Hour)
	handlers := mustNewHandlers(t, fakeEventReader{
		listRelayHealthFn: func(_ context.Context) ([]model.IngestCheckpoint, error) {
			return []model.IngestCheckpoint{
				{
					RelayURL:    "wss://relay.one",
					Mode:        "live",
					FilterGroup: "social_core",
					Status:      "healthy",
					UpdatedAt:   healthyAt,
				},
				{
					RelayURL:    "wss://relay.damus.io",
					Mode:        "live",
					FilterGroup: "default_v1",
					Status:      "errored",
					LastError:   &lastErr,
					UpdatedAt:   erroredAt,
				},
				{
					RelayURL:    "wss://relay.abandoned",
					Mode:        "live",
					FilterGroup: "default_v1",
					Status:      "healthy",
					UpdatedAt:   staleAt,
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
		Total  int                `json:"total"`
		Scope  string             `json:"scope"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Total != 3 || resp.Scope != "ingest_checkpoints" {
		t.Fatalf("unexpected checkpoint count: total=%d scope=%s", resp.Total, resp.Scope)
	}
	if len(resp.Relays) != 3 || resp.Relays[0].RelayURL != "wss://relay.one" {
		t.Fatalf("unexpected relays payload: %+v", resp.Relays)
	}
	if resp.Relays[0].Status != "healthy" || resp.Relays[0].Mode != "live" || resp.Relays[0].Stale {
		t.Fatalf("expected a current live checkpoint to stay healthy, got %+v", resp.Relays[0])
	}
	if !resp.Relays[0].LatestCheckpointAt.Equal(healthyAt) {
		t.Fatalf("expected healthy latest_checkpoint_at %v, got %v", healthyAt, resp.Relays[0].LatestCheckpointAt)
	}
	if resp.Relays[0].LastError != nil {
		t.Fatalf("expected healthy relay last_error to be omitted, got %+v", resp.Relays[0].LastError)
	}
	if resp.Relays[1].Status != "errored" {
		t.Fatalf("expected errored status, got %+v", resp.Relays[1])
	}
	if resp.Relays[1].LastError == nil || *resp.Relays[1].LastError != lastErr {
		t.Fatalf("expected last_error %q, got %+v", lastErr, resp.Relays[1].LastError)
	}
	if !resp.Relays[1].LatestCheckpointAt.Equal(erroredAt) {
		t.Fatalf("expected errored latest_checkpoint_at %v, got %v", erroredAt, resp.Relays[1].LatestCheckpointAt)
	}
	if resp.Relays[2].Status != "healthy" || !resp.Relays[2].Stale || resp.Relays[2].Mode != "live" {
		t.Fatalf("expected abandoned checkpoint to keep status healthy, mode live, and stale=true, got %+v", resp.Relays[2])
	}
}
