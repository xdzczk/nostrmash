package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBatchGetEvents_SuccessWithExplicitMissingIDs(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				ids[0]: json.RawMessage(`{"id":"evt_1","kind":1}`),
				ids[2]: json.RawMessage(`{"id":"evt_3","kind":1}`),
			}, nil
		},
	}, 10)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", strings.NewReader(`{"ids":["evt_1","evt_2","evt_3"]}`))
	rec := httptest.NewRecorder()
	handlers.BatchGetEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Events  []json.RawMessage `json:"events"`
		Missing []string          `json:"missing"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(resp.Events))
	}
	if len(resp.Missing) != 1 || resp.Missing[0] != "evt_2" {
		t.Fatalf("expected missing to contain evt_2, got %#v", resp.Missing)
	}
}
