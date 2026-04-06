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

func TestRequestPathTemplate_UsesServeMuxPattern(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/abc", nil)
	req.Pattern = "GET /api/v1/events/{id}"
	if got := requestPathTemplate(req); got != "/api/v1/events/{id}" {
		t.Fatalf("unexpected path template: got %q", got)
	}
}

func TestWithHTTPRateLimit_ExemptsHealthAndLimitsSearch(t *testing.T) {
	limited := WithHTTPRateLimit(HTTPRateLimitOptions{
		DefaultRPM:   1,
		DefaultBurst: 1,
		SearchRPM:    1,
	}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthReq.RemoteAddr = "127.0.0.1:1234"
	healthRec := httptest.NewRecorder()
	limited.ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusNoContent {
		t.Fatalf("unexpected health status: got %d want %d", healthRec.Code, http.StatusNoContent)
	}

	searchReq1 := httptest.NewRequest(http.MethodGet, "/api/v1/search?q=x", nil)
	searchReq1.RemoteAddr = "127.0.0.1:4321"
	searchRec1 := httptest.NewRecorder()
	limited.ServeHTTP(searchRec1, searchReq1)
	if searchRec1.Code != http.StatusNoContent {
		t.Fatalf("unexpected first search status: got %d want %d", searchRec1.Code, http.StatusNoContent)
	}

	searchReq2 := httptest.NewRequest(http.MethodGet, "/api/v1/search?q=x", nil)
	searchReq2.RemoteAddr = "127.0.0.1:4321"
	searchRec2 := httptest.NewRecorder()
	limited.ServeHTTP(searchRec2, searchReq2)
	if searchRec2.Code != http.StatusTooManyRequests {
		t.Fatalf("unexpected second search status: got %d want %d", searchRec2.Code, http.StatusTooManyRequests)
	}
}

func TestWithPanicRecovery_ReturnsInternalErrorEnvelope(t *testing.T) {
	handler := WithRequestID(WithPanicRecovery(nil, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/1", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusInternalServerError)
	}
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.Error.Code != "internal_error" {
		t.Fatalf("unexpected code: got %q", envelope.Error.Code)
	}
	if envelope.Error.RequestID == "" {
		t.Fatalf("expected request id in error envelope")
	}
}
