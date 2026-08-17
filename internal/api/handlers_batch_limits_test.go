package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestBatchGetEvents_RejectsOversizedPayload(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 200)
	tooLargeJSON := `{"ids":["` + strings.Repeat("a", publicBatchBodyLimitBytes+10) + `"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", strings.NewReader(tooLargeJSON))
	rec := httptest.NewRecorder()
	handlers.BatchGetEvents(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestBatchGetProfiles_RejectsOversizedPayload(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 200)
	tooLargeJSON := `{"pubkeys":["` + strings.Repeat("a", publicBatchBodyLimitBytes+10) + `"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/profiles/batch", strings.NewReader(tooLargeJSON))
	rec := httptest.NewRecorder()
	handlers.BatchGetProfiles(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestBatchGetEvents_BackendErrorReturnsDegradedMissing(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, _ []string) (map[string]json.RawMessage, error) {
			return nil, errors.New("db timeout")
		},
	}, 200)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", strings.NewReader(`{"ids":["evt_a","evt_b"]}`))
	rec := httptest.NewRecorder()
	handlers.BatchGetEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body batchEventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.Degraded {
		t.Fatalf("expected degraded batch, got %#v", body)
	}
	if len(body.Events) != 0 {
		t.Fatalf("expected empty events, got %#v", body.Events)
	}
	if len(body.Missing) != 2 {
		t.Fatalf("expected requested ids marked missing, got %#v", body.Missing)
	}
}

func TestBatchGetProfiles_BackendErrorReturnsDegradedMissing(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getProfilesByBatch: func(_ context.Context, _ []string) (map[string]store.ProfileProjection, error) {
			return nil, errors.New("db timeout")
		},
	}, 200)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/profiles/batch", strings.NewReader(`{"pubkeys":["pk_a","pk_b"]}`))
	rec := httptest.NewRecorder()
	handlers.BatchGetProfiles(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body batchProfilesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.Degraded {
		t.Fatalf("expected degraded batch, got %#v", body)
	}
	if len(body.Profiles) != 0 {
		t.Fatalf("expected empty profiles, got %#v", body.Profiles)
	}
	if len(body.MissingPubkeys) != 2 {
		t.Fatalf("expected requested pubkeys marked missing, got %#v", body.MissingPubkeys)
	}
}
