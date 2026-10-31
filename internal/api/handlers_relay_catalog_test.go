package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetRelayCatalog_PoolNil(t *testing.T) {
	h := mustNewHandlersWithOptions(t, fakeEventReader{}, HandlersOptions{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/relays/catalog", nil)
	rec := httptest.NewRecorder()
	h.GetRelayCatalog(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not_available") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGetPopularRelays_PoolNil(t *testing.T) {
	h := mustNewHandlersWithOptions(t, fakeEventReader{}, HandlersOptions{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/relays/popular", nil)
	rec := httptest.NewRecorder()
	h.GetPopularRelays(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not_available") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGetPopularRelays_InvalidLimit(t *testing.T) {
	h := mustNewHandlersWithOptions(t, fakeEventReader{}, HandlersOptions{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/relays/popular?limit=201", nil)
	rec := httptest.NewRecorder()
	h.GetPopularRelays(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestGetRelayCatalog_InvalidLimit(t *testing.T) {
	h := mustNewHandlersWithOptions(t, fakeEventReader{}, HandlersOptions{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/relays/catalog?limit=101", nil)
	rec := httptest.NewRecorder()
	h.GetRelayCatalog(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}
