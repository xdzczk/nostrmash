package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetProfilePublicSummary_ReturnsProductShapedPayload(t *testing.T) {
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
			recent := int64(456)
			return store.ProfilePublicStatsProjection{
				Pubkey:           "pk_1",
				FollowerCount:    10,
				FollowingCount:   7,
				NoteCount:        40,
				ReplyCount:       8,
				RecentActivityAt: &recent,
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/users/{pubkey}/summary", handlers.GetProfilePublicSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/pk_1/summary", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var payload profilePublicSummaryResponse
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Pubkey != "pk_1" || payload.MetadataEventID != "evt_meta_1" || payload.MetadataCreatedAt != 123 {
		t.Fatalf("unexpected summary profile envelope: %#v", payload)
	}
	if payload.Stats.FollowerCount != 10 || payload.Stats.FollowingCount != 7 || payload.Stats.NoteCount != 40 || payload.Stats.ReplyCount != 8 {
		t.Fatalf("unexpected summary stats: %#v", payload.Stats)
	}
	if payload.Stats.RecentActivityAt == nil || *payload.Stats.RecentActivityAt != 456 {
		t.Fatalf("unexpected recent activity: %#v", payload.Stats.RecentActivityAt)
	}
}

func TestGetProfilePublicSummary_MissingProfileReturnsNotFound(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getProfileByPubkey: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{}, store.ErrNotFound
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/users/{pubkey}/summary", handlers.GetProfilePublicSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/missing/summary", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNotFound)
	}
}
