package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetEventAncestors_IncludesMissingAncestorIDs(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getEventAncestors: func(_ context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
			if eventID != "evt_child" {
				t.Fatalf("unexpected event id: %s", eventID)
			}
			if maxDepth != 4 {
				t.Fatalf("unexpected max depth: %d", maxDepth)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_root"}`)}, []string{"evt_missing_parent"}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}/ancestors", handlers.GetEventAncestors)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_child/ancestors?max_depth=4", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Missing []string `json:"missing_ancestor_ids"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Missing) != 1 || resp.Missing[0] != "evt_missing_parent" {
		t.Fatalf("unexpected missing ids: %#v", resp.Missing)
	}
}
