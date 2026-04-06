package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetMentions_ReturnsReferencedEvents(t *testing.T) {
	handlers := NewHandlers(fakeEventReader{
		getRefsPubkeyFn: func(_ context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
			if pubkey != "pk1" {
				t.Fatalf("unexpected pubkey: %s", pubkey)
			}
			if limit != 10 {
				t.Fatalf("unexpected limit: %d", limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_mention_1"}`)}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/users/{pubkey}/mentions", handlers.GetMentions)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/pk1/mentions?limit=10", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
}
