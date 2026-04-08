package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetEventActions_UsesSharedServiceAndPreservesPrimalShape(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getEventCountsFn: func(_ context.Context, eventID string) (store.EventCounts, error) {
			if eventID != "evt_actions_1" {
				t.Fatalf("unexpected event id: %q", eventID)
			}
			return store.EventCounts{
				EventID:       "evt_actions_1",
				ReplyCount:    2,
				ReactionCount: 3,
				RepostCount:   4,
				Consistency:   "eventual",
			}, nil
		},
	}, 10)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/v1/events/{id}/actions", handlers.GetEventActions)

	req := httptest.NewRequest(http.MethodGet, "/primal/v1/events/evt_actions_1/actions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		EventID       string `json:"event_id"`
		ReplyCount    int64  `json:"reply_count"`
		ReactionCount int64  `json:"reaction_count"`
		RepostCount   int64  `json:"repost_count"`
		Consistency   string `json:"consistency"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.EventID != "evt_actions_1" ||
		resp.ReplyCount != 2 ||
		resp.ReactionCount != 3 ||
		resp.RepostCount != 4 ||
		resp.Consistency != "eventual" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestGetEventActions_StoreErrorStillReturnsInternalError(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getEventCountsFn: func(_ context.Context, _ string) (store.EventCounts, error) {
			return store.EventCounts{}, errors.New("storage down")
		},
	}, 10)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/v1/events/{id}/actions", handlers.GetEventActions)

	req := httptest.NewRequest(http.MethodGet, "/primal/v1/events/evt_store_err/actions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestGetEventActions_EmptyEventIDReturnsBadRequest(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)
	req := httptest.NewRequest(http.MethodGet, "/primal/v1/events/actions", nil)
	req.SetPathValue("id", "   ")
	rec := httptest.NewRecorder()
	handlers.GetEventActions(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
	}
}
