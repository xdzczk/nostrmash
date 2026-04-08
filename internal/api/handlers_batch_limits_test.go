package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
