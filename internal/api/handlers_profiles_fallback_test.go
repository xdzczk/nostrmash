package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetProfileByPubkey_LocalMissRelayFallbackSuccess(t *testing.T) {
	handlers := mustNewHandlersWithOptions(t, fakeEventReader{
		getProfileByPubkey: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{}, store.ErrNotFound
		},
	}, HandlersOptions{
		MaxBatchSize: 10,
		QueryOptions: query.ServiceOptions{
			FallbackReader: apiFakeFallbackReader{
				fetchProfilesByPubkeysFn: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
					return map[string]store.ProfileProjection{
						"pk_1": {
							Pubkey:            "pk_1",
							MetadataEventID:   "evt_meta_1",
							MetadataCreatedAt: 100,
							ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
						},
					}, nil
				},
			},
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/profiles/{pubkey}", handlers.GetProfileByPubkey)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/profiles/pk_1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var payload profileResponse
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Pubkey != "pk_1" {
		t.Fatalf("unexpected fallback profile pubkey: %q", payload.Pubkey)
	}
}
