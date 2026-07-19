package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
	storeread "github.com/xdzczk/nostrmash/internal/store/read"
)

func TestGetProfileByPubkey_AcceptsNpub(t *testing.T) {
	const hexPubkey = "e3b5f43280264c6d1d2e4376e5c5de645ae66bc580fa454f01b711e7f5bee9d0"
	npub := encodeNpub(hexPubkey)
	if npub == "" {
		t.Fatal("expected encodeNpub to succeed")
	}

	var seenPubkey string
	handlers := mustNewHandlers(t, fakeEventReader{
		getProfileByPubkey: func(_ context.Context, pubkey string) (store.ProfileProjection, error) {
			seenPubkey = pubkey
			if pubkey != hexPubkey {
				return store.ProfileProjection{}, store.ErrNotFound
			}
			return store.ProfileProjection{
				Pubkey:            hexPubkey,
				MetadataEventID:   "evt_meta",
				MetadataCreatedAt: 1,
				ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/profiles/{pubkey}", handlers.GetProfileByPubkey)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/profiles/"+npub, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if seenPubkey != hexPubkey {
		t.Fatalf("expected handler to canonicalize npub to hex, got %q", seenPubkey)
	}
}

func TestGetRelatedProfiles_AcceptsNpub(t *testing.T) {
	const hexPubkey = "e3b5f43280264c6d1d2e4376e5c5de645ae66bc580fa454f01b711e7f5bee9d0"
	npub := encodeNpub(hexPubkey)
	if npub == "" {
		t.Fatal("expected encodeNpub to succeed")
	}

	var seenPubkey string
	handlers := mustNewHandlers(t, fakeEventReader{
		getRelatedProfilesFn: func(_ context.Context, pubkey string, _ int) ([]storeread.RelatedProfile, error) {
			seenPubkey = pubkey
			if pubkey != hexPubkey {
				return nil, store.ErrNotFound
			}
			return []storeread.RelatedProfile{{Pubkey: "pk_related", Score: 1}}, nil
		},
		getProfilesByBatch: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/profiles/{pubkey}/related", handlers.GetRelatedProfiles)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/"+npub+"/related", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if seenPubkey != hexPubkey {
		t.Fatalf("expected related lookup to use hex, got %q", seenPubkey)
	}
}

func TestGetProfilePublicSummary_RelatedTimeoutDoesNotFailSummary(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getProfileByPubkey: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{
				Pubkey:            "pk_1",
				MetadataEventID:   "evt_meta_1",
				MetadataCreatedAt: 123,
				ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
			}, nil
		},
		getProfilePublicStatsByPubkey: func(context.Context, string) (store.ProfilePublicStatsProjection, error) {
			return store.ProfilePublicStatsProjection{
				Pubkey:        "pk_1",
				FollowerCount: 10,
				NoteCount:     40,
			}, nil
		},
		getRelatedProfilesFn: func(ctx context.Context, _ string, _ int) ([]storeread.RelatedProfile, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		getRisingProfilesFn: func(ctx context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingProfile, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/users/{pubkey}/summary", handlers.GetProfilePublicSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/pk_1/summary", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var payload profilePublicSummaryResponse
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Stats.NoteCount != 40 {
		t.Fatalf("expected hero stats to survive enrichment timeout, got %#v", payload.Stats)
	}
	if len(payload.RelatedDiscovery.RelatedProfiles) != 0 || len(payload.RelatedDiscovery.RisingProfiles) != 0 {
		t.Fatalf("expected empty related discovery after timeout, got %#v", payload.RelatedDiscovery)
	}
}
