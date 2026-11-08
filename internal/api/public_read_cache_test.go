package api

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublicReadCacheServesTheSecondRequestFromMemory(t *testing.T) {
	var calls atomic.Int32
	cache := NewPublicReadCache(PublicReadCacheConfig{TTL: time.Minute, MaxEntries: 8})
	handler := cache.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/abc", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		if rec.Header().Get("Cache-Control") != "public, max-age=60" {
			t.Fatalf("cache-control %q", rec.Header().Get("Cache-Control"))
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d", calls.Load())
	}
}

func TestPublicReadCacheRateLimitsOneAddress(t *testing.T) {
	cache := NewPublicReadCache(PublicReadCacheConfig{RatePerSecond: 1, Burst: 1, TTL: time.Minute})
	handler := cache.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/v1/threads/abc", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first status %d", first.Code)
	}
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/api/v1/threads/def", nil))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status %d", second.Code)
	}
}

func TestPublicReadCacheSkipsIdentifiedRequests(t *testing.T) {
	var calls atomic.Int32
	cache := NewPublicReadCache(PublicReadCacheConfig{TTL: time.Minute})
	handler := cache.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/threads/abc", nil)
		req.Header.Set("Authorization", "Bearer secret")
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}
	if calls.Load() != 2 {
		t.Fatalf("identified requests were cached: %d", calls.Load())
	}
}
