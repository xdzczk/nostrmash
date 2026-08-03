package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetAuthorEvents_SortsAlreadyProjectedOrder(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorEventsFn: func(_ context.Context, _ string, limit int) ([]json.RawMessage, error) {
			if limit != 20 {
				t.Fatalf("unexpected default limit: %d", limit)
			}
			// Store enrichment attaches engagement fields before the handler;
			// the API must pass them through unchanged.
			return []json.RawMessage{
				json.RawMessage(`{"id":"newest","created_at":1002,"reply_count":3,"repost_count":1,"reaction_count":5,"zap_count":2,"zap_msats":1000}`),
				json.RawMessage(`{"id":"tie_b","created_at":1000,"reply_count":0,"repost_count":0,"reaction_count":0,"zap_count":0,"zap_msats":0}`),
				json.RawMessage(`{"id":"tie_a","created_at":1000,"reply_count":0,"repost_count":0,"reaction_count":0,"zap_count":0,"zap_msats":0}`),
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/events", handlers.GetAuthorEvents)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/events", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var payload struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Events) != 3 {
		t.Fatalf("unexpected event count: got=%d want=3", len(payload.Events))
	}
	if payload.Events[0]["reply_count"] != float64(3) || payload.Events[0]["reaction_count"] != float64(5) {
		t.Fatalf("expected engagement counts to pass through, got %#v", payload.Events[0])
	}
}

func TestGetAuthorReplies_ReturnsItems(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorRepliesFn: func(_ context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
			if pubkey != "pubkey_x" || limit != 20 {
				t.Fatalf("unexpected args: pubkey=%s limit=%d", pubkey, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"reply_1"}`)}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/replies", handlers.GetAuthorReplies)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/replies", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Pubkey string            `json:"pubkey"`
		Items  []json.RawMessage `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pubkey != "pubkey_x" || len(resp.Items) != 1 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestGetEventCounts_ExposesEventualConsistency(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getEventCountsFn: func(_ context.Context, id string) (store.EventCounts, error) {
			return store.EventCounts{
				EventID:       id,
				ReplyCount:    3,
				ReactionCount: 4,
				RepostCount:   2,
				Consistency:   "eventual",
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events/{id}/counts", handlers.GetEventCounts)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt_1/counts", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Consistency string `json:"consistency"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Consistency != "eventual" {
		t.Fatalf("unexpected consistency value: got %q want %q", resp.Consistency, "eventual")
	}
}
