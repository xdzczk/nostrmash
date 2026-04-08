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
		getHotConversationsFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.HotConversation, error) {
			if window != 24*time.Hour {
				t.Fatalf("unexpected conversations window: %s", window)
			}
			if limit != 2 {
				t.Fatalf("unexpected conversations limit: %d", limit)
			}
			if offset != 1 {
				t.Fatalf("unexpected conversations offset: %d", offset)
			}
			return []store.HotConversation{
				{RootEventID: "root_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hot one", ReplyCount: 4, ParticipantCount: 3, LastActivityAt: 1700000100, Replies24h: 4, Replies7d: 5, VelocityScore: 4.3, Consistency: "eventual"},
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
	mux.HandleFunc("GET /api/v1/discovery/conversations/hot", h.GetHotConversations)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/trending", h.GetTrendingHashtags)
	mux.HandleFunc("GET /api/v1/discovery/profiles/trending", h.GetTrendingProfiles)
	mux.HandleFunc("GET /api/v1/discovery/profiles/rising", h.GetRisingProfiles)

	paths := []string{
		"/api/v1/discovery/notes/trending?window=24h&limit=2",
		"/api/v1/discovery/conversations/hot?window=24h&limit=2&offset=1",
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

func TestDiscoveryHomeRoute_ComposesBoundedSections(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.TrendingNote, error) {
			if window != 24*time.Hour || limit != 3 || offset != 0 {
				t.Fatalf("unexpected notes args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []store.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello", Score: 2.5},
			}, nil
		},
		getTrendingTagsFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.TrendingHashtag, error) {
			if window != 24*time.Hour || limit != 2 || offset != 0 {
				t.Fatalf("unexpected hashtags args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []store.TrendingHashtag{
				{Hashtag: "nostr", EventCount: 5, UniqueAuthors: 4},
			}, nil
		},
		getTrendingProfilesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.TrendingProfile, error) {
			if window != 24*time.Hour || limit != 2 || offset != 0 {
				t.Fatalf("unexpected trending profiles args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []store.TrendingProfile{{Pubkey: "pk_a", Score: 9.1}}, nil
		},
		getRisingProfilesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.TrendingProfile, error) {
			if window != 24*time.Hour || limit != 2 || offset != 0 {
				t.Fatalf("unexpected rising profiles args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []store.TrendingProfile{{Pubkey: "pk_b", Score: 7.4}}, nil
		},
		getPublicNetworkStatsFn: func(_ context.Context, hashtagLimit int) (store.PublicDiscoveryNetworkStats, error) {
			if hashtagLimit != 7 {
				t.Fatalf("unexpected hashtag stat limit: %d", hashtagLimit)
			}
			return store.PublicDiscoveryNetworkStats{
				EventsIngested:    11,
				ProjectedProfiles: 6,
				Relays:            3,
				ActiveAuthors:     store.WindowedCount{Last24h: 4, Last7d: 8},
				NoteVolume:        store.WindowedCount{Last24h: 12, Last7d: 40},
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/home", h.GetDiscoveryHome)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/home?window=24h&notes_limit=3&hashtags_limit=2&profiles_limit=2&hashtag_stat_limit=7", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["surface"] != "home" {
		t.Fatalf("unexpected surface: %#v", body["surface"])
	}
	sections, ok := body["sections"].(map[string]any)
	if !ok {
		t.Fatalf("missing sections payload: %#v", body)
	}
	if _, ok := sections["trending_notes"].([]any); !ok {
		t.Fatalf("missing trending_notes section: %#v", sections)
	}
	if _, ok := sections["trending_hashtags"].([]any); !ok {
		t.Fatalf("missing trending_hashtags section: %#v", sections)
	}
	profiles, ok := sections["profiles"].(map[string]any)
	if !ok {
		t.Fatalf("missing profiles section: %#v", sections)
	}
	if _, ok := profiles["trending"].([]any); !ok {
		t.Fatalf("missing profiles.trending section: %#v", profiles)
	}
	if _, ok := profiles["rising"].([]any); !ok {
		t.Fatalf("missing profiles.rising section: %#v", profiles)
	}
	if _, ok := sections["network_summary"].(map[string]any); !ok {
		t.Fatalf("missing network_summary section: %#v", sections)
	}
}

func TestDiscoveryHomeRoute_RendersSparseSectionWithoutDroppingBundle(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.TrendingNote, error) {
			return []store.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello"},
			}, nil
		},
		getTrendingTagsFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.TrendingHashtag, error) {
			return []store.TrendingHashtag{}, nil
		},
		getTrendingProfilesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.TrendingProfile, error) {
			return []store.TrendingProfile{{Pubkey: "pk_a", Score: 9}}, nil
		},
		getRisingProfilesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.TrendingProfile, error) {
			return []store.TrendingProfile{}, nil
		},
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (store.PublicDiscoveryNetworkStats, error) {
			return store.PublicDiscoveryNetworkStats{
				EventsIngested:    1,
				ProjectedProfiles: 1,
				Relays:            1,
				ActiveAuthors:     store.WindowedCount{},
				NoteVolume:        store.WindowedCount{},
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/home", h.GetDiscoveryHome)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/home", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	sections, ok := body["sections"].(map[string]any)
	if !ok {
		t.Fatalf("missing sections payload: %#v", body)
	}
	hashtags, ok := sections["trending_hashtags"].([]any)
	if !ok {
		t.Fatalf("missing trending_hashtags section: %#v", sections)
	}
	if len(hashtags) != 0 {
		t.Fatalf("expected sparse hashtag section to be empty, got %#v", hashtags)
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
				TopLanguages24h: []store.LanguageSummary{{Language: "en", Count: 5}},
				TopLanguages7d:  []store.LanguageSummary{{Language: "en", Count: 9}},
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
			TopLanguages map[string][]store.LanguageSummary `json:"top_languages"`
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
	if len(networkResp.Network.TopLanguages["24h"]) == 0 || networkResp.Network.TopLanguages["24h"][0].Language != "en" {
		t.Fatalf("unexpected top_languages payload: %#v", networkResp.Network.TopLanguages)
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
		getHotConversationsFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.HotConversation, error) {
			return nil, errors.Join(query.ErrUnsupportedCapability, errors.New("query: hot conversations unsupported"))
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
	mux.HandleFunc("GET /api/v1/discovery/conversations/hot", h.GetHotConversations)
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

	badConversationsWindowReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/conversations/hot?window=48h", nil)
	badConversationsWindowRec := httptest.NewRecorder()
	mux.ServeHTTP(badConversationsWindowRec, badConversationsWindowReq)
	if badConversationsWindowRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad conversations window: got %d want %d", badConversationsWindowRec.Code, http.StatusBadRequest)
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

	unsupportedConversationsReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/conversations/hot?window=24h&limit=2", nil)
	unsupportedConversationsRec := httptest.NewRecorder()
	mux.ServeHTTP(unsupportedConversationsRec, unsupportedConversationsReq)
	if unsupportedConversationsRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status for unsupported conversations: got %d want %d", unsupportedConversationsRec.Code, http.StatusNotImplemented)
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

func TestHashtagPageRoutes_SummaryNotesAndRelated(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getHashtagSummaryFn: func(_ context.Context, hashtag string) (store.HashtagSummary, error) {
			if hashtag != "nostr" {
				t.Fatalf("unexpected hashtag summary key: %s", hashtag)
			}
			latest := int64(1700000010)
			return store.HashtagSummary{
				Hashtag:       "nostr",
				LatestEventAt: &latest,
				Activity: store.HashtagActivityStats{
					Last24h: store.HashtagActivity{EventCount: 3, UniqueAuthors: 2},
					Last7d:  store.HashtagActivity{EventCount: 7, UniqueAuthors: 4},
					Last30d: store.HashtagActivity{EventCount: 9, UniqueAuthors: 5},
					All:     store.HashtagActivity{EventCount: 11, UniqueAuthors: 6},
				},
			}, nil
		},
		getHashtagNotesFn: func(_ context.Context, hashtag string, sort string, window string, limit int, offset int) ([]store.TrendingNote, error) {
			if hashtag != "nostr" || sort != "top" || window != "7d" || limit != 2 || offset != 1 {
				t.Fatalf("unexpected hashtag notes args: %s %s %s %d %d", hashtag, sort, window, limit, offset)
			}
			return []store.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello", ReplyCount: 1, RepostCount: 2, ReactionCount: 3, ZapCount: 1, ZapMSats: 2000, Score: 9.2},
			}, nil
		},
		getRelatedHashtagsFn: func(_ context.Context, hashtag string, limit int) ([]store.RelatedHashtag, error) {
			if hashtag != "nostr" || limit != 3 {
				t.Fatalf("unexpected related hashtag args: %s %d", hashtag, limit)
			}
			return []store.RelatedHashtag{
				{Hashtag: "bitcoin", CoOccurrenceCount: 4, CoOccurrenceAuthors: 3},
			}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}", h.GetHashtagSummary)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}/notes", h.GetHashtagNotes)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}/related", h.GetRelatedHashtags)

	for _, path := range []string{
		"/api/v1/discovery/hashtags/nostr",
		"/api/v1/discovery/hashtags/nostr/notes?sort=top&window=7d&limit=2&offset=1",
		"/api/v1/discovery/hashtags/nostr/related?limit=3",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status for %s: got %d want %d", path, rec.Code, http.StatusOK)
		}
	}
}

func TestHashtagPageRoutes_NormalizationMissingAndValidation(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getHashtagSummaryFn: func(_ context.Context, hashtag string) (store.HashtagSummary, error) {
			if hashtag == "nostr" {
				return store.HashtagSummary{Hashtag: "nostr", Activity: store.HashtagActivityStats{All: store.HashtagActivity{EventCount: 1, UniqueAuthors: 1}}}, nil
			}
			return store.HashtagSummary{}, store.ErrNotFound
		},
		getHashtagNotesFn: func(_ context.Context, hashtag string, _, _ string, _, _ int) ([]store.TrendingNote, error) {
			if hashtag == "missing" {
				return nil, store.ErrNotFound
			}
			return []store.TrendingNote{}, nil
		},
		getRelatedHashtagsFn: func(_ context.Context, hashtag string, _ int) ([]store.RelatedHashtag, error) {
			if hashtag == "missing" {
				return nil, store.ErrNotFound
			}
			return []store.RelatedHashtag{}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}", h.GetHashtagSummary)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}/notes", h.GetHashtagNotes)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}/related", h.GetRelatedHashtags)

	okReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/%23Nostr", nil)
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for normalized hashtag: got %d want %d", okRec.Code, http.StatusOK)
	}

	missingReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/missing", nil)
	missingRec := httptest.NewRecorder()
	mux.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status for missing hashtag summary: got %d want %d", missingRec.Code, http.StatusNotFound)
	}

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/###bad/notes?sort=wat&window=99d", nil)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for invalid notes params: got %d want %d", badRec.Code, http.StatusBadRequest)
	}
}

func TestDomainPageRoutes_TrendingSummaryAndNotes(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingDomainsFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.DomainSummaryProjection, error) {
			if window != 7*24*time.Hour || limit != 2 || offset != 1 {
				t.Fatalf("unexpected trending domains args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []store.DomainSummaryProjection{
				{
					Domain: "example.com",
					Activity: store.DomainActivityStatsProjection{
						Last24h: store.DomainActivityProjection{LinkCount: 3, NoteCount: 2, UniqueAuthors: 2},
						Last7d:  store.DomainActivityProjection{LinkCount: 7, NoteCount: 5, UniqueAuthors: 4},
					},
				},
			}, nil
		},
		getDomainSummaryFn: func(_ context.Context, domain string, recentLimit int, topLimit int) (store.DomainSummaryProjection, error) {
			if domain != "example.com" || recentLimit != 5 || topLimit != 5 {
				t.Fatalf("unexpected domain summary args: %s %d %d", domain, recentLimit, topLimit)
			}
			latest := int64(1700000011)
			return store.DomainSummaryProjection{
				Domain:        "example.com",
				LatestEventAt: &latest,
				Activity: store.DomainActivityStatsProjection{
					Last24h: store.DomainActivityProjection{LinkCount: 2, NoteCount: 2, UniqueAuthors: 2},
					Last7d:  store.DomainActivityProjection{LinkCount: 8, NoteCount: 6, UniqueAuthors: 5},
					Last30d: store.DomainActivityProjection{LinkCount: 11, NoteCount: 8, UniqueAuthors: 6},
					All:     store.DomainActivityProjection{LinkCount: 13, NoteCount: 10, UniqueAuthors: 7},
				},
				RecentNotes: []store.TrendingNote{
					{EventID: "note_recent", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "recent"},
				},
				TopNotes: []store.TrendingNote{
					{EventID: "note_top", AuthorPubkey: "pk_2", CreatedAt: 1699999999, Content: "top", Score: 12.2},
				},
			}, nil
		},
		getDomainNotesFn: func(_ context.Context, domain string, sort string, window string, limit int, offset int) ([]store.TrendingNote, error) {
			if domain != "example.com" || sort != "top" || window != "30d" || limit != 2 || offset != 1 {
				t.Fatalf("unexpected domain notes args: %s %s %s %d %d", domain, sort, window, limit, offset)
			}
			return []store.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello", Score: 9.2},
			}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/domains/trending", h.GetTrendingDomains)
	mux.HandleFunc("GET /api/v1/discovery/domains/{domain}", h.GetDomainSummary)
	mux.HandleFunc("GET /api/v1/discovery/domains/{domain}/notes", h.GetDomainNotes)

	for _, path := range []string{
		"/api/v1/discovery/domains/trending?window=7d&limit=2&offset=1",
		"/api/v1/discovery/domains/example.com",
		"/api/v1/discovery/domains/example.com/notes?sort=top&window=30d&limit=2&offset=1",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status for %s: got %d want %d", path, rec.Code, http.StatusOK)
		}
	}
}

func TestDomainPageRoutes_NormalizationMissingAndValidation(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getDomainSummaryFn: func(_ context.Context, domain string, _, _ int) (store.DomainSummaryProjection, error) {
			if domain == "example.com" {
				return store.DomainSummaryProjection{
					Domain:   domain,
					Activity: store.DomainActivityStatsProjection{All: store.DomainActivityProjection{LinkCount: 1, NoteCount: 1, UniqueAuthors: 1}},
				}, nil
			}
			return store.DomainSummaryProjection{}, store.ErrNotFound
		},
		getDomainNotesFn: func(_ context.Context, domain string, _, _ string, _, _ int) ([]store.TrendingNote, error) {
			if domain == "missing.example" {
				return nil, store.ErrNotFound
			}
			return []store.TrendingNote{}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/domains/{domain}", h.GetDomainSummary)
	mux.HandleFunc("GET /api/v1/discovery/domains/{domain}/notes", h.GetDomainNotes)

	okReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/domains/HTTPS:%2F%2FExample.com", nil)
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for normalized domain: got %d want %d", okRec.Code, http.StatusOK)
	}

	missingReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/domains/missing.example", nil)
	missingRec := httptest.NewRecorder()
	mux.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status for missing domain summary: got %d want %d", missingRec.Code, http.StatusNotFound)
	}

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/domains/###bad/notes?sort=wat&window=99d", nil)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for invalid domain notes params: got %d want %d", badRec.Code, http.StatusBadRequest)
	}
}
