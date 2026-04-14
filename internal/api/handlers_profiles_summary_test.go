package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
	if payload.Hero.DisplayName != "alice" || payload.Hero.Handle != "alice" {
		t.Fatalf("unexpected hero identity fields: %#v", payload.Hero)
	}
	if payload.Hero.Counters.NoteCount != 40 || payload.Hero.Counters.ReplyCount != 8 {
		t.Fatalf("unexpected hero counters: %#v", payload.Hero.Counters)
	}
	if payload.Hero.Metadata.NpubOrPubkey.Raw != "pk_1" {
		t.Fatalf("unexpected hero metadata strip: %#v", payload.Hero.Metadata)
	}
	if len(payload.Hero.Actions) != 3 {
		t.Fatalf("unexpected hero actions: %#v", payload.Hero.Actions)
	}
	if len(payload.RecentNotes) != 0 {
		t.Fatalf("expected no recent notes fallback, got %#v", payload.RecentNotes)
	}
	if len(payload.RecentNotePreviews) != 0 {
		t.Fatalf("expected no recent note previews fallback, got %#v", payload.RecentNotePreviews)
	}
	if len(payload.RelatedDiscovery.RelatedProfiles) != 0 || len(payload.RelatedDiscovery.RisingProfiles) != 0 {
		t.Fatalf("expected empty related discovery, got %#v", payload.RelatedDiscovery)
	}
	if payload.IdentityDetails.Title == "" || len(payload.IdentityDetails.Fields) == 0 {
		t.Fatalf("expected identity details module, got %#v", payload.IdentityDetails)
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

func TestGetProfilePublicSummary_IncludesRecentAndDiscoverySections(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getProfileByPubkey: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{
				Pubkey:            "pk_target",
				MetadataEventID:   "evt_meta_target",
				MetadataCreatedAt: 321,
				ProfileJSON:       json.RawMessage(`{"name":"target","display_name":"Target User","about":"Long bio goes here","website":"https://target.example","lud16":"target@ln.example"}`),
			}, nil
		},
		getProfilePublicStatsByPubkey: func(context.Context, string) (store.ProfilePublicStatsProjection, error) {
			return store.ProfilePublicStatsProjection{
				Pubkey:         "pk_target",
				FollowerCount:  12,
				FollowingCount: 13,
				NoteCount:      55,
				ReplyCount:     9,
			}, nil
		},
		getAuthorEventsFn: func(context.Context, string, int) ([]json.RawMessage, error) {
			return []json.RawMessage{
				json.RawMessage(`{"id":"note_1","pubkey":"pk_target","content":"hello world"}`),
			}, nil
		},
		getRelatedProfilesFn: func(context.Context, string, int) ([]store.RelatedProfile, error) {
			return []store.RelatedProfile{
				{Pubkey: "pk_related", Score: 99, Reasons: []string{"topic_overlap"}},
			}, nil
		},
		getRisingProfilesFn: func(context.Context, time.Duration, int, int) ([]store.TrendingProfile, error) {
			return []store.TrendingProfile{
				{Pubkey: "pk_rising", Score: 88},
			}, nil
		},
		getProfilesByBatch: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				"pk_related": {Pubkey: "pk_related", ProfileJSON: json.RawMessage(`{"name":"related-user"}`)},
				"pk_rising":  {Pubkey: "pk_rising", ProfileJSON: json.RawMessage(`{"display_name":"Rising User"}`)},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/users/{pubkey}/summary", handlers.GetProfilePublicSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/pk_target/summary", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var payload profilePublicSummaryResponse
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.RecentNotes) != 1 {
		t.Fatalf("expected recent notes to be present, got %#v", payload.RecentNotes)
	}
	if len(payload.RecentNotePreviews) != 1 {
		t.Fatalf("expected recent note previews to be present, got %#v", payload.RecentNotePreviews)
	}
	if payload.RecentNotePreviews[0]["event_id"] != "note_1" {
		t.Fatalf("unexpected recent note preview payload: %#v", payload.RecentNotePreviews[0])
	}
	if len(payload.RelatedDiscovery.RelatedProfiles) != 1 || len(payload.RelatedDiscovery.RisingProfiles) != 1 {
		t.Fatalf("expected related discovery sections, got %#v", payload.RelatedDiscovery)
	}
	if payload.Hero.DisplayName != "Target User" || payload.Hero.Bio == "" {
		t.Fatalf("unexpected hero content: %#v", payload.Hero)
	}
	if payload.Hero.Metadata.Website == nil || payload.Hero.Metadata.LUD16 == nil {
		t.Fatalf("expected hero metadata website/lud16, got %#v", payload.Hero.Metadata)
	}
}
