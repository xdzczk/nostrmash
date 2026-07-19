package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	storeread "github.com/xdzczk/nostrmash/internal/store/read"
)

func TestBuildPublicCacheKey_NormalizesAndSortsParams(t *testing.T) {
	keyA := buildPublicCacheKey("discovery:example", map[string]any{
		"q":      "  nostr   mash ",
		"limit":  10,
		"offset": 2,
	})
	keyB := buildPublicCacheKey("discovery:example", map[string]any{
		"offset": 2,
		"q":      "nostr mash",
		"limit":  10,
	})

	if keyA != keyB {
		t.Fatalf("expected equivalent normalized keys, got %q and %q", keyA, keyB)
	}
}

func TestDiscoveryCache_KeyNormalizationForHashtagSummary(t *testing.T) {
	calls := 0
	cacheEnabled := true
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getHashtagSummaryFn: func(_ context.Context, _ string) (storeread.HashtagSummary, error) {
			calls++
			return storeread.HashtagSummary{
				Hashtag: "nostr",
				Activity: storeread.HashtagActivityStats{
					All: storeread.HashtagActivity{EventCount: 1, UniqueAuthors: 1},
				},
			}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:      &cacheEnabled,
			MaxEntries:   8,
			DiscoveryTTL: time.Minute,
		},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}", h.GetHashtagSummary)

	for _, path := range []string{
		"/api/v1/discovery/hashtags/%23Nostr",
		"/api/v1/discovery/hashtags/nostr",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status for %s: got %d want %d", path, rec.Code, http.StatusOK)
		}
	}

	if calls != 1 {
		t.Fatalf("expected one backend call for normalized hashtag cache key, got %d", calls)
	}
}

func TestDiscoveryCache_HitMissObserver(t *testing.T) {
	cacheEnabled := true
	var lookups []string
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingNote, error) {
			return []storeread.TrendingNote{
				{EventID: "evt_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "ok"},
			}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:      &cacheEnabled,
			MaxEntries:   8,
			DiscoveryTTL: time.Minute,
		},
		CacheLookupObserver: func(family, endpoint string, hit bool) {
			result := "miss"
			if hit {
				result = "hit"
			}
			lookups = append(lookups, family+":"+endpoint+":"+result)
		},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=1&offset=0", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
		}
	}

	if len(lookups) != 2 || lookups[0] != "discovery:trending_notes:miss" || lookups[1] != "discovery:trending_notes:hit" {
		t.Fatalf("unexpected lookup observer sequence: %#v", lookups)
	}
}
