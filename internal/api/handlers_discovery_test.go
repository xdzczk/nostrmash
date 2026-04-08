package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestDiscoveryTrendingRoutes_ReturnSuccess(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.TrendingNote, error) {
			if window != 24*time.Hour {
				t.Fatalf("unexpected notes window: %s", window)
			}
			if limit != 2 {
				t.Fatalf("unexpected notes limit: %d", limit)
			}
			if offset != 0 {
				t.Fatalf("unexpected notes offset: %d", offset)
			}
			return []store.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello", ReplyCount: 4, RepostCount: 2, ReactionCount: 3, ZapCount: 1, ZapMSats: 20000, Score: 12.5},
				{EventID: "note_2", AuthorPubkey: "pk_2", CreatedAt: 1700000010, Content: "world", ReplyCount: 1, RepostCount: 0, ReactionCount: 2, ZapCount: 0, ZapMSats: 0, Score: 4.25},
			}, nil
		},
		getTrendingTagsFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.TrendingHashtag, error) {
			if window != 7*24*time.Hour {
				t.Fatalf("unexpected hashtag window: %s", window)
			}
			if limit != 3 {
				t.Fatalf("unexpected hashtag limit: %d", limit)
			}
			if offset != 1 {
				t.Fatalf("unexpected hashtag offset: %d", offset)
			}
			return []store.TrendingHashtag{{Hashtag: "nostr", EventCount: 11, UniqueAuthors: 6}, {Hashtag: "bitcoin", EventCount: 8, UniqueAuthors: 5}}, nil
		},
		getTrendingProfilesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.TrendingProfile, error) {
			if window != 24*time.Hour {
				t.Fatalf("unexpected trending profiles window: %s", window)
			}
			if limit != 4 {
				t.Fatalf("unexpected trending profiles limit: %d", limit)
			}
			if offset != 2 {
				t.Fatalf("unexpected trending profiles offset: %d", offset)
			}
			return []store.TrendingProfile{{Pubkey: "pk_a", Score: 9.5}, {Pubkey: "pk_b", Score: 6.25}}, nil
		},
		getRisingProfilesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.TrendingProfile, error) {
			if window != 7*24*time.Hour {
				t.Fatalf("unexpected rising profiles window: %s", window)
			}
			if limit != 2 {
				t.Fatalf("unexpected rising profiles limit: %d", limit)
			}
			if offset != 1 {
				t.Fatalf("unexpected rising profiles offset: %d", offset)
			}
			return []store.TrendingProfile{{Pubkey: "pk_c", Score: 5.75}}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/trending", h.GetTrendingHashtags)
	mux.HandleFunc("GET /api/v1/discovery/profiles/trending", h.GetTrendingProfiles)
	mux.HandleFunc("GET /api/v1/discovery/profiles/rising", h.GetRisingProfiles)

	paths := []string{
		"/api/v1/discovery/notes/trending?window=24h&limit=2",
		"/api/v1/discovery/hashtags/trending?window=7d&limit=3&offset=1",
		"/api/v1/discovery/profiles/trending?window=24h&limit=4&offset=2",
		"/api/v1/discovery/profiles/rising?window=7d&limit=2&offset=1",
	}
	for _, path := range paths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status for %s: got %d want %d", path, rec.Code, http.StatusOK)
		}
	}
}

func TestDiscoveryStatsRoutes_ReturnSuccess(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getPublicNetworkStatsFn: func(_ context.Context, hashtagLimit int) (store.PublicDiscoveryNetworkStats, error) {
			if hashtagLimit != 10 && hashtagLimit != 1 {
				t.Fatalf("unexpected hashtag limit: got=%d want one of [10,1]", hashtagLimit)
			}
			return store.PublicDiscoveryNetworkStats{
				EventsIngested:    11,
				ProjectedProfiles: 7,
				Relays:            3,
				ActiveAuthors:     store.WindowedCount{Last24h: 5, Last7d: 9},
				NoteVolume:        store.WindowedCount{Last24h: 12, Last7d: 44},
				TopHashtags: &store.TrendingHashtagWindows{
					Last24h: []store.TrendingHashtag{{Hashtag: "nostr", EventCount: 6, UniqueAuthors: 4}},
					Last7d:  []store.TrendingHashtag{{Hashtag: "nostr", EventCount: 10, UniqueAuthors: 8}},
				},
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/stats/network", h.GetNetworkStats)
	mux.HandleFunc("GET /api/v1/discovery/stats/content", h.GetContentStats)
	mux.HandleFunc("GET /api/v1/discovery/stats/relays", h.GetRelayStats)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status for network stats: got %d want %d", rec.Code, http.StatusOK)
	}
	var networkResp struct {
		Network struct {
			Totals struct {
				EventsIngested    int64 `json:"events_ingested"`
				ProjectedProfiles int64 `json:"projected_profiles"`
			} `json:"totals"`
			Activity struct {
				ActiveAuthors store.WindowedCount `json:"active_authors"`
				NoteVolume    store.WindowedCount `json:"note_volume"`
			} `json:"activity"`
			Relays struct {
				Total int64 `json:"total"`
			} `json:"relays"`
		} `json:"network"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &networkResp); err != nil {
		t.Fatalf("decode network response: %v", err)
	}
	if networkResp.Network.Totals.EventsIngested != 11 || networkResp.Network.Totals.ProjectedProfiles != 7 {
		t.Fatalf("unexpected network totals payload: %#v", networkResp.Network.Totals)
	}
	if networkResp.Network.Activity.ActiveAuthors.Last24h != 5 || networkResp.Network.Activity.NoteVolume.Last7d != 44 {
		t.Fatalf("unexpected activity payload: %#v", networkResp.Network.Activity)
	}
	if networkResp.Network.Relays.Total != 3 {
		t.Fatalf("unexpected relay total payload: %#v", networkResp.Network.Relays)
	}

	contentReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/content", nil)
	contentRec := httptest.NewRecorder()
	mux.ServeHTTP(contentRec, contentReq)
	if contentRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for content stats: got %d want %d", contentRec.Code, http.StatusOK)
	}

	relayReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/relays", nil)
	relayRec := httptest.NewRecorder()
	mux.ServeHTTP(relayRec, relayReq)
	if relayRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for relays stats: got %d want %d", relayRec.Code, http.StatusOK)
	}
}

func TestDiscoveryRoutes_BadLimitAndUnsupportedCapability(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.TrendingNote, error) {
			return nil, errors.Join(query.ErrUnsupportedCapability, errors.New("query: curated recommended reads unsupported"))
		},
		getTrendingProfilesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.TrendingProfile, error) {
			return nil, errors.Join(query.ErrUnsupportedCapability, errors.New("query: trending profiles unsupported"))
		},
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (store.PublicDiscoveryNetworkStats, error) {
			return store.PublicDiscoveryNetworkStats{}, errors.Join(query.ErrUnsupportedCapability, errors.New("query: network stats unsupported"))
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/trending", h.GetTrendingHashtags)
	mux.HandleFunc("GET /api/v1/discovery/profiles/trending", h.GetTrendingProfiles)
	mux.HandleFunc("GET /api/v1/discovery/stats/network", h.GetNetworkStats)

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?limit=1000", nil)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad limit: got %d want %d", badRec.Code, http.StatusBadRequest)
	}

	badNotesWindowReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=48h", nil)
	badNotesWindowRec := httptest.NewRecorder()
	mux.ServeHTTP(badNotesWindowRec, badNotesWindowReq)
	if badNotesWindowRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad notes window: got %d want %d", badNotesWindowRec.Code, http.StatusBadRequest)
	}

	badWindowReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/trending?window=48h", nil)
	badWindowRec := httptest.NewRecorder()
	mux.ServeHTTP(badWindowRec, badWindowReq)
	if badWindowRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad window: got %d want %d", badWindowRec.Code, http.StatusBadRequest)
	}

	badProfileWindowReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/trending?window=48h", nil)
	badProfileWindowRec := httptest.NewRecorder()
	mux.ServeHTTP(badProfileWindowRec, badProfileWindowReq)
	if badProfileWindowRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad profile window: got %d want %d", badProfileWindowRec.Code, http.StatusBadRequest)
	}

	unsupportedNotesReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=2", nil)
	unsupportedNotesRec := httptest.NewRecorder()
	mux.ServeHTTP(unsupportedNotesRec, unsupportedNotesReq)
	if unsupportedNotesRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status for unsupported notes: got %d want %d", unsupportedNotesRec.Code, http.StatusNotImplemented)
	}

	unsupportedProfilesReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/trending?window=24h&limit=2", nil)
	unsupportedProfilesRec := httptest.NewRecorder()
	mux.ServeHTTP(unsupportedProfilesRec, unsupportedProfilesReq)
	if unsupportedProfilesRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status for unsupported profiles: got %d want %d", unsupportedProfilesRec.Code, http.StatusNotImplemented)
	}

	unsupportedStatsReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	unsupportedStatsRec := httptest.NewRecorder()
	mux.ServeHTTP(unsupportedStatsRec, unsupportedStatsReq)
	if unsupportedStatsRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status for unsupported stats: got %d want %d", unsupportedStatsRec.Code, http.StatusNotImplemented)
	}
}

func TestDiscoveryStatsRoutes_MissingDataEdgeCases(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (store.PublicDiscoveryNetworkStats, error) {
			return store.PublicDiscoveryNetworkStats{
				EventsIngested:    0,
				ProjectedProfiles: 0,
				Relays:            0,
				ActiveAuthors:     store.WindowedCount{Last24h: 0, Last7d: 0},
				NoteVolume:        store.WindowedCount{Last24h: 0, Last7d: 0},
				TopHashtags:       nil,
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/stats/network", h.GetNetworkStats)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	networkValue, ok := decoded["network"].(map[string]any)
	if !ok {
		t.Fatalf("missing network payload: %#v", decoded)
	}
	if _, hasTopHashtags := networkValue["top_hashtags"]; hasTopHashtags {
		t.Fatalf("top_hashtags should be omitted when unavailable: %#v", networkValue)
	}
}

func TestDiscoveryCache_HitAndMissForTrendingNotes(t *testing.T) {
	calls := 0
	cacheEnabled := true
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.TrendingNote, error) {
			calls++
			return []store.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "cached", Score: 1.0},
			}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:     &cacheEnabled,
			MaxEntries:  8,
			TrendingTTL: time.Minute,
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=1&offset=0", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status on request %d: got %d want %d", i+1, rec.Code, http.StatusOK)
		}
	}
	if calls != 1 {
		t.Fatalf("expected one backend call for cache hit path, got %d", calls)
	}
}

func TestDiscoveryCache_SeparatesKeysByParams(t *testing.T) {
	calls := 0
	cacheEnabled := true
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.TrendingNote, error) {
			calls++
			return []store.TrendingNote{{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "ok", Score: 1.0}}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:     &cacheEnabled,
			MaxEntries:  8,
			TrendingTTL: time.Minute,
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)

	reqA := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=1&offset=0", nil)
	recA := httptest.NewRecorder()
	mux.ServeHTTP(recA, reqA)
	if recA.Code != http.StatusOK {
		t.Fatalf("unexpected status for first key: got %d want %d", recA.Code, http.StatusOK)
	}

	reqB := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=2&offset=0", nil)
	recB := httptest.NewRecorder()
	mux.ServeHTTP(recB, reqB)
	if recB.Code != http.StatusOK {
		t.Fatalf("unexpected status for second key: got %d want %d", recB.Code, http.StatusOK)
	}

	if calls != 2 {
		t.Fatalf("expected separate backend calls for different params, got %d", calls)
	}
}

func TestDiscoveryCache_TTLExpiry(t *testing.T) {
	calls := 0
	cacheEnabled := true
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (store.PublicDiscoveryNetworkStats, error) {
			calls++
			return store.PublicDiscoveryNetworkStats{
				EventsIngested: 12,
				Relays:         4,
			}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:        &cacheEnabled,
			MaxEntries:     8,
			PublicStatsTTL: 10 * time.Millisecond,
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/stats/network", h.GetNetworkStats)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status before expiry: got %d want %d", rec.Code, http.StatusOK)
	}

	time.Sleep(20 * time.Millisecond)

	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("unexpected status after expiry: got %d want %d", rec2.Code, http.StatusOK)
	}
	if calls != 2 {
		t.Fatalf("expected cache expiry to trigger second backend call, got %d", calls)
	}
}

func TestDiscoveryCache_DisabledFallsBackToQueryPath(t *testing.T) {
	calls := 0
	cacheEnabled := false
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (store.PublicDiscoveryNetworkStats, error) {
			calls++
			return store.PublicDiscoveryNetworkStats{Relays: 2}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:        &cacheEnabled,
			MaxEntries:     8,
			PublicStatsTTL: time.Minute,
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/stats/relays", h.GetRelayStats)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/relays", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status on request %d: got %d want %d", i+1, rec.Code, http.StatusOK)
		}
	}

	if calls != 2 {
		t.Fatalf("expected cache-disabled path to call backend twice, got %d", calls)
	}
}

func TestDiscoveryTrendingRoutes_TrustMetadataByMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                string
		mode                string
		expectedApplied     bool
		expectedResultScope string
	}{
		{name: "open", mode: "open", expectedApplied: false, expectedResultScope: "open"},
		{name: "prefer_trusted", mode: "prefer_trusted", expectedApplied: true, expectedResultScope: "prefer_trusted"},
		{name: "trusted_only", mode: "trusted_only", expectedApplied: true, expectedResultScope: "trusted_only"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			h := mustNewHandlersWithOptions(t, trustQualifiedFakeReader{
				fakeEventReader: fakeEventReader{
					getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.TrendingNote, error) {
						return []store.TrendingNote{
							{EventID: "note_1", AuthorPubkey: "pk_trusted", CreatedAt: 1700000000, Content: "hello"},
							{EventID: "note_2", AuthorPubkey: "pk_open", CreatedAt: 1700000001, Content: "world"},
						}, nil
					},
				},
				getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ store.TrustQualificationPolicy) (map[string]store.TrustQualification, error) {
					out := make(map[string]store.TrustQualification, len(pubkeys))
					for _, pubkey := range pubkeys {
						out[pubkey] = store.TrustQualification{Pubkey: pubkey, Trusted: pubkey == "pk_trusted"}
					}
					return out, nil
				},
				isTrustedAuthorFn: func(_ context.Context, pubkey string, _ store.TrustQualificationPolicy) (bool, error) {
					return pubkey == "pk_trusted", nil
				},
			}, HandlersOptions{
				MaxBatchSize: 200,
				QueryOptions: query.ServiceOptions{
					DiscoveryCandidateTrustMode: tc.mode,
				},
			})

			req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=2", nil)
			rec := httptest.NewRecorder()
			http.HandlerFunc(h.GetTrendingNotes).ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
			}

			var body struct {
				TrustMode    string `json:"trust_mode"`
				TrustApplied bool   `json:"trust_applied"`
				ResultScope  string `json:"result_scope"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if body.TrustMode != tc.mode {
				t.Fatalf("unexpected trust_mode: got %q want %q", body.TrustMode, tc.mode)
			}
			if body.TrustApplied != tc.expectedApplied {
				t.Fatalf("unexpected trust_applied: got %v want %v", body.TrustApplied, tc.expectedApplied)
			}
			if body.ResultScope != tc.expectedResultScope {
				t.Fatalf("unexpected result_scope: got %q want %q", body.ResultScope, tc.expectedResultScope)
			}
		})
	}
}
