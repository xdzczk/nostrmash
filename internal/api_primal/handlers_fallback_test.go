package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestPrimalGetEventByID_LocalMissRelayFallbackSuccess(t *testing.T) {
	handlers := mustNewHandlersWithOptions(t, fakeEventReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, HandlersOptions{
		MaxBatchSize: 10,
		QueryOptions: query.ServiceOptions{
			FallbackReader: query.AdaptFallbackReader(primalFakeFallbackReader{
				fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
					return map[string]json.RawMessage{
						"evt_1": json.RawMessage(`{"id":"evt_1","kind":1}`),
					}, nil
				},
			}),
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/primal/v1/events/evt_1", nil)
	req.SetPathValue("id", "evt_1")
	rec := httptest.NewRecorder()
	handlers.GetEventByID(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusOK)
	}
	var payload struct {
		Event map[string]any `json:"event"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Event["id"] != "evt_1" {
		t.Fatalf("unexpected fallback event payload: %#v", payload.Event)
	}
}

func TestPrimalGetEventByID_LocalMissRelayMissPreservesNotFound(t *testing.T) {
	handlers := mustNewHandlersWithOptions(t, fakeEventReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, HandlersOptions{
		MaxBatchSize: 10,
		QueryOptions: query.ServiceOptions{
			FallbackReader: query.AdaptFallbackReader(primalFakeFallbackReader{
				fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
					return map[string]json.RawMessage{}, nil
				},
			}),
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/primal/v1/events/evt_missing", nil)
	req.SetPathValue("id", "evt_missing")
	rec := httptest.NewRecorder()
	handlers.GetEventByID(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusNotFound)
	}
}

func TestPrimalGetProfileByPubkey_LocalMissRelayFallbackSuccess(t *testing.T) {
	handlers := mustNewHandlersWithOptions(t, fakeEventReader{
		getProfileByPubkey: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{}, store.ErrNotFound
		},
	}, HandlersOptions{
		MaxBatchSize: 10,
		QueryOptions: query.ServiceOptions{
			FallbackReader: query.AdaptFallbackReader(primalFakeFallbackReader{
				fetchProfilesByPubkeysFn: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
					return map[string]store.ProfileProjection{
						"pk_1": {
							Pubkey:            "pk_1",
							MetadataEventID:   "meta_1",
							MetadataCreatedAt: 100,
							ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
						},
					}, nil
				},
			}),
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/primal/v1/profiles/pk_1", nil)
	req.SetPathValue("pubkey", "pk_1")
	rec := httptest.NewRecorder()
	handlers.GetProfileByPubkey(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusOK)
	}
	var payload struct {
		Pubkey string `json:"pubkey"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Pubkey != "pk_1" {
		t.Fatalf("unexpected profile payload: %#v", payload)
	}
}

type primalFakeFallbackReader struct {
	fetchEventsByIDsFn       func(context.Context, []string) (map[string]json.RawMessage, error)
	fetchProfilesByPubkeysFn func(context.Context, []string) (map[string]store.ProfileProjection, error)
}

func (f primalFakeFallbackReader) FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.fetchEventsByIDsFn == nil {
		return map[string]json.RawMessage{}, nil
	}
	return f.fetchEventsByIDsFn(ctx, ids)
}

func (f primalFakeFallbackReader) FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
	if f.fetchProfilesByPubkeysFn == nil {
		return map[string]store.ProfileProjection{}, nil
	}
	return f.fetchProfilesByPubkeysFn(ctx, pubkeys)
}
