package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type scriptedAcquirer struct {
	delay time.Duration
	err   error
	calls int
}

func (s *scriptedAcquirer) Acquire(ctx context.Context) (func(), error) {
	s.calls++
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	return func() {}, nil
}

func TestWithDBAcquireBudget_ServesWhenAConnectionIsFree(t *testing.T) {
	acquirer := &scriptedAcquirer{}
	var served bool
	handler := withDBAcquireBudget(acquirer, 200*time.Millisecond, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served = true
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/events/abc", nil))
	if !served || rec.Code != http.StatusNoContent {
		t.Fatalf("expected the handler to run, served=%v status=%d", served, rec.Code)
	}
	if acquirer.calls != 1 {
		t.Fatalf("expected one checkout, got %d", acquirer.calls)
	}
}

func TestWithDBAcquireBudget_RejectsWhenThePoolStaysBusy(t *testing.T) {
	acquirer := &scriptedAcquirer{delay: time.Second, err: context.DeadlineExceeded}
	var served bool
	handler := withDBAcquireBudget(acquirer, 30*time.Millisecond, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served = true
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/profiles/abc", nil))
	if served {
		t.Fatal("handler ran after the acquire budget expired")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", rec.Header().Get("Retry-After"))
	}
}

func TestWithDBAcquireBudget_SkipsHealth(t *testing.T) {
	acquirer := &scriptedAcquirer{err: errors.New("should not be called")}
	handler := withDBAcquireBudget(acquirer, 20*time.Millisecond, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if acquirer.calls != 0 {
		t.Fatalf("health checked out a connection %d times", acquirer.calls)
	}
}
