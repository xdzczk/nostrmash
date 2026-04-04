package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/logging"
)

func TestWithRequestID_PropagatesIncomingHeader(t *testing.T) {
	var captured string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = logging.RequestIDFromContext(r.Context())
	})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Request-ID", "incoming-id-42")
	rec := httptest.NewRecorder()

	WithRequestID(next).ServeHTTP(rec, req)

	if captured != "incoming-id-42" {
		t.Fatalf("unexpected context request id: got %q", captured)
	}
	if got := rec.Header().Get("X-Request-ID"); got != "incoming-id-42" {
		t.Fatalf("unexpected response request id: got %q", got)
	}
}

func TestWithRequestID_GeneratesWhenHeaderMissing(t *testing.T) {
	var captured string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = logging.RequestIDFromContext(r.Context())
	})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	WithRequestID(next).ServeHTTP(rec, req)

	if captured == "" || captured == "unknown" {
		t.Fatalf("expected generated request id, got %q", captured)
	}
	if got := rec.Header().Get("X-Request-ID"); got != captured {
		t.Fatalf("response request id mismatch: got %q want %q", got, captured)
	}
}

func TestRequireBearerToken_MissingToken(t *testing.T) {
	protected := RequireBearerToken("secret-token", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/system", nil)
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusUnauthorized)
	}

	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.Error.Code != "unauthorized" {
		t.Fatalf("unexpected code: got %q", envelope.Error.Code)
	}
}

func TestRequireBearerToken_AllowsValidToken(t *testing.T) {
	protected := RequireBearerToken("secret-token", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/system", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNoContent)
	}
}
