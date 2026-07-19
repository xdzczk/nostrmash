package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
	storeread "github.com/xdzczk/nostrmash/internal/store/read"
)

func TestGetAuthorAnalyticsSummary_ReturnsWindows(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorAnalyticsSummaryFn: func(_ context.Context, pubkey string) ([]store.AuthorAnalyticsSummaryProjection, error) {
			if pubkey != "pubkey_x" {
				t.Fatalf("unexpected pubkey: %s", pubkey)
			}
			return []store.AuthorAnalyticsSummaryProjection{
				{
					Pubkey:             pubkey,
					WindowDays:         7,
					PostCount:          10,
					NoteCount:          6,
					ReplyCount:         4,
					EngagementReceived: 20,
					EngagementGiven:    15,
					QuoteRepost: store.AuthorQuoteRepostWindowProjection{
						QuotesMade:      2,
						RepostsMade:     3,
						QuotesReceived:  5,
						RepostsReceived: 8,
					},
				},
			}, nil
		},
		getAuthorRelayFootprintFn: func(_ context.Context, pubkey string, topRelayLimit int) (store.AuthorRelayFootprintProjection, error) {
			if pubkey != "pubkey_x" || topRelayLimit != 8 {
				t.Fatalf("unexpected relay footprint args: pubkey=%s topRelayLimit=%d", pubkey, topRelayLimit)
			}
			return store.AuthorRelayFootprintProjection{
				Pubkey:           pubkey,
				RelayCount:       3,
				SeenOnEventCount: 6,
				TopRelays: []storeread.RelayUsageSummary{
					{RelayURL: "wss://relay.a", EventCount: 4, UniqueAuthors: 1},
					{RelayURL: "wss://relay.b", EventCount: 2, UniqueAuthors: 1},
				},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/summary", handlers.GetAuthorAnalyticsSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/summary", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Pubkey         string `json:"pubkey"`
		RelayFootprint struct {
			RelayCount       int64 `json:"relay_count"`
			SeenOnEventCount int64 `json:"seen_on_event_count"`
			TopRelays        []struct {
				RelayURL   string `json:"relay_url"`
				EventCount int64  `json:"event_count"`
			} `json:"top_relays"`
		} `json:"relay_footprint"`
		Windows []struct {
			Window      string `json:"window"`
			QuoteRepost struct {
				QuotesMade      float64 `json:"quotes_made"`
				RepostsMade     float64 `json:"reposts_made"`
				QuotesReceived  float64 `json:"quotes_received"`
				RepostsReceived float64 `json:"reposts_received"`
			} `json:"quote_repost"`
		} `json:"windows"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pubkey != "pubkey_x" || len(resp.Windows) != 1 || resp.Windows[0].Window != "7d" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Windows[0].QuoteRepost.QuotesMade != 2 ||
		resp.Windows[0].QuoteRepost.RepostsMade != 3 ||
		resp.Windows[0].QuoteRepost.QuotesReceived != 5 ||
		resp.Windows[0].QuoteRepost.RepostsReceived != 8 {
		t.Fatalf("unexpected quote/repost rollup: %+v", resp.Windows[0].QuoteRepost)
	}
	if resp.RelayFootprint.RelayCount != 3 ||
		resp.RelayFootprint.SeenOnEventCount != 6 ||
		len(resp.RelayFootprint.TopRelays) != 2 ||
		resp.RelayFootprint.TopRelays[0].RelayURL != "wss://relay.a" {
		t.Fatalf("unexpected relay footprint payload: %+v", resp.RelayFootprint)
	}
}

func TestGetAuthorAnalyticsTopics_ValidatesWindow(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/topics", handlers.GetAuthorAnalyticsTopics)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/topics?window=2d", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGetAuthorAnalyticsTopics_DefaultWindowAndRollup(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorTopicStatsFn: func(_ context.Context, pubkey string, windowDays int, limit int) ([]store.AuthorTopicStatsProjection, error) {
			if pubkey != "pubkey_x" || windowDays != 30 || limit != 20 {
				t.Fatalf("unexpected args: pubkey=%s window=%d limit=%d", pubkey, windowDays, limit)
			}
			return []store.AuthorTopicStatsProjection{
				{Hashtag: "nostr", UsageCount: 5, ActiveDays: 3},
				{Hashtag: "bitcoin", UsageCount: 2, ActiveDays: 2},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/topics", handlers.GetAuthorAnalyticsTopics)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/topics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Pubkey string `json:"pubkey"`
		Window string `json:"window"`
		Items  []struct {
			Hashtag    string `json:"hashtag"`
			UsageCount int64  `json:"usage_count"`
			ActiveDays int    `json:"active_days"`
		} `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pubkey != "pubkey_x" || resp.Window != "30d" || len(resp.Items) != 2 {
		t.Fatalf("unexpected response envelope: %+v", resp)
	}
	if resp.Items[0].Hashtag != "nostr" || resp.Items[0].UsageCount != 5 || resp.Items[0].ActiveDays != 3 {
		t.Fatalf("unexpected top topic row: %+v", resp.Items[0])
	}
}

func TestGetAuthorAnalyticsGroupedNotes_DefaultHashtagGrouping(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getGroupedNoteAnalyticsFn: func(_ context.Context, req store.GroupedNoteAnalyticsQuery) (store.GroupedNoteAnalyticsProjection, error) {
			if req.Pubkey != "pubkey_x" {
				t.Fatalf("unexpected pubkey: %s", req.Pubkey)
			}
			if req.GroupKind != "hashtag" || req.GroupKey != "nostr" {
				t.Fatalf("unexpected grouping request: %#v", req)
			}
			if req.WindowDays != 30 || req.TopNotesLimit != 5 || req.TopicsLimit != 5 {
				t.Fatalf("unexpected limits/window request: %#v", req)
			}
			return store.GroupedNoteAnalyticsProjection{
				Pubkey:     req.Pubkey,
				WindowDays: req.WindowDays,
				GroupKind:  req.GroupKind,
				GroupKey:   req.GroupKey,
				NoteCount:  2,
				Engagement: store.GroupedEngagementTotalsProjection{
					ReplyCount:    3,
					ReactionCount: 4,
					RepostCount:   1,
					ZapCount:      2,
					ZapMSats:      5000,
				},
				Media: store.GroupedMediaSummaryProjection{
					TotalPosts:           2,
					WithImageCount:       1,
					WithVideoCount:       0,
					WithLinkCount:        1,
					WithArticleCount:     0,
					TextOnlyCount:        0,
					TotalAttachmentCount: 2,
				},
				TopNotes: []store.GroupedTopNoteProjection{
					{EventID: "n1", WeightedEngagement: 10},
				},
				TopTopics: []store.GroupedTopicSummaryProjection{
					{Hashtag: "nostr", UsageCount: 2, ActiveDays: 2},
				},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/grouped-notes", handlers.GetAuthorAnalyticsGroupedNotes)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/grouped-notes?group_key=nostr", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Pubkey    string `json:"pubkey"`
		Window    string `json:"window"`
		GroupKind string `json:"group_kind"`
		GroupKey  string `json:"group_key"`
		NoteCount int64  `json:"note_count"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pubkey != "pubkey_x" || resp.Window != "30d" || resp.GroupKind != "hashtag" || resp.GroupKey != "nostr" || resp.NoteCount != 2 {
		t.Fatalf("unexpected grouped notes response: %+v", resp)
	}
}

func TestGetAuthorAnalyticsGroupedNotes_MetadataAndValidation(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getGroupedNoteAnalyticsFn: func(_ context.Context, req store.GroupedNoteAnalyticsQuery) (store.GroupedNoteAnalyticsProjection, error) {
			if req.GroupKind != "metadata" || req.MetadataTag != "d" || req.GroupKey != "series-42" || req.WindowDays != 7 {
				t.Fatalf("unexpected metadata request: %#v", req)
			}
			return store.GroupedNoteAnalyticsProjection{
				Pubkey:      req.Pubkey,
				WindowDays:  req.WindowDays,
				GroupKind:   req.GroupKind,
				GroupKey:    req.GroupKey,
				MetadataTag: req.MetadataTag,
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/grouped-notes", handlers.GetAuthorAnalyticsGroupedNotes)

	okReq := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/authors/pubkey_x/analytics/grouped-notes?group_by=metadata&metadata_tag=d&group_key=series-42&window=7d",
		nil,
	)
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for metadata grouping: got %d want %d", okRec.Code, http.StatusOK)
	}

	badReq := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/authors/pubkey_x/analytics/grouped-notes?group_by=metadata&group_key=series-42",
		nil,
	)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for missing metadata_tag: got %d want %d", badRec.Code, http.StatusBadRequest)
	}
}

func TestGetAuthorAnalyticsMediaMix_DefaultWindow(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorMediaMixStatsFn: func(_ context.Context, pubkey string, windowDays int) (store.AuthorMediaMixStatsProjection, error) {
			if pubkey != "pubkey_x" || windowDays != 30 {
				t.Fatalf("unexpected args: pubkey=%s window=%d", pubkey, windowDays)
			}
			return store.AuthorMediaMixStatsProjection{
				Pubkey:         pubkey,
				WindowDays:     windowDays,
				TotalPosts:     10,
				WithImageCount: 3,
				WithVideoCount: 2,
				WithLinkCount:  4,
				TextOnlyCount:  1,
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/media-mix", handlers.GetAuthorAnalyticsMediaMix)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/media-mix", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Pubkey string `json:"pubkey"`
		Window string `json:"window"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pubkey != "pubkey_x" || resp.Window != "30d" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestGetAuthorAnalyticsActivityWindows_DefaultWindow(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorActivityWindowBucketsFn: func(_ context.Context, pubkey string, windowDays int) ([]store.AuthorActivityWindowBucketProjection, error) {
			if pubkey != "pubkey_x" || windowDays != 30 {
				t.Fatalf("unexpected args: pubkey=%s window=%d", pubkey, windowDays)
			}
			return []store.AuthorActivityWindowBucketProjection{
				{
					Pubkey:             pubkey,
					WindowDays:         windowDays,
					DayOfWeek:          1,
					HourOfDay:          10,
					EngagementReceived: 5,
					ReplyReceived:      2,
				},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/activity-windows", handlers.GetAuthorAnalyticsActivityWindows)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/activity-windows", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Pubkey   string `json:"pubkey"`
		Window   string `json:"window"`
		Timezone string `json:"timezone"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pubkey != "pubkey_x" || resp.Window != "30d" || resp.Timezone != "UTC" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestGetAuthorAnalyticsPostingPatterns_ValidatesWindow(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/posting-patterns", handlers.GetAuthorAnalyticsPostingPatterns)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/posting-patterns?window=2d", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGetAuthorAnalyticsTopNotes_OrdersByWeightedEngagement(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorTopNotesFn: func(_ context.Context, pubkey string, windowDays int, limit int) ([]store.AuthorTopNoteProjection, error) {
			if pubkey != "pubkey_x" || windowDays != 30 || limit != 10 {
				t.Fatalf("unexpected args: pubkey=%s window=%d limit=%d", pubkey, windowDays, limit)
			}
			return []store.AuthorTopNoteProjection{
				{EventID: "n2", WeightedEngagement: 30.5},
				{EventID: "n1", WeightedEngagement: 20.0},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/top-notes", handlers.GetAuthorAnalyticsTopNotes)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/top-notes?window=30d", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Window string `json:"window"`
		Items  []struct {
			EventID            string  `json:"event_id"`
			WeightedEngagement float64 `json:"weighted_engagement"`
		} `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Window != "30d" || len(resp.Items) != 2 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Items[0].EventID != "n2" || resp.Items[0].WeightedEngagement < resp.Items[1].WeightedEngagement {
		t.Fatalf("unexpected ordering in top notes: %+v", resp.Items)
	}
}

func TestGetAuthorAnalyticsPerformanceSummary_WindowBehaviorAndTotals(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorPerformanceAggregateFn: func(_ context.Context, pubkey string, windowDays int) (store.AuthorPerformanceAggregateProjection, store.AuthorPerformanceAggregateProjection, error) {
			if pubkey != "pubkey_x" || windowDays != 7 {
				t.Fatalf("unexpected args: pubkey=%s window=%d", pubkey, windowDays)
			}
			return store.AuthorPerformanceAggregateProjection{
					NoteCount:                 2,
					TotalWeightedEngagement:   42.0,
					AverageWeightedEngagement: 21.0,
					MedianWeightedEngagement:  21.0,
					TotalReplyCount:           5,
					TotalReactionCount:        10,
					TotalRepostCount:          2,
					TotalZapCount:             1,
					TotalZapMSats:             1000,
					AverageReplyCount:         2.5,
					AverageReactionCount:      5.0,
					AverageRepostCount:        1.0,
					AverageZapCount:           0.5,
					MedianReplyCount:          2.5,
					MedianReactionCount:       5.0,
					MedianRepostCount:         1.0,
					MedianZapCount:            0.5,
				}, store.AuthorPerformanceAggregateProjection{
					NoteCount:                 1,
					TotalWeightedEngagement:   30.0,
					AverageWeightedEngagement: 30.0,
					MedianWeightedEngagement:  30.0,
				}, nil
		},
		getAuthorMediaMixStatsFn: func(_ context.Context, pubkey string, windowDays int) (store.AuthorMediaMixStatsProjection, error) {
			return store.AuthorMediaMixStatsProjection{Pubkey: pubkey, WindowDays: windowDays, TotalPosts: 2, WithImageCount: 1}, nil
		},
		getAuthorTopicStatsFn: func(_ context.Context, pubkey string, windowDays int, limit int) ([]store.AuthorTopicStatsProjection, error) {
			return []store.AuthorTopicStatsProjection{{Hashtag: "nostr", UsageCount: 2}}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/performance-summary", handlers.GetAuthorAnalyticsPerformanceSummary)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/performance-summary?window=7d", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Window  string `json:"window"`
		Summary struct {
			NoteCount               float64 `json:"note_count"`
			TotalWeightedEngagement float64 `json:"total_weighted_engagement"`
			Comparison              struct {
				NoteCountDelta float64 `json:"note_count_delta"`
			} `json:"comparison"`
		} `json:"summary"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Window != "7d" {
		t.Fatalf("expected 7d window, got %+v", resp)
	}
	if resp.Summary.NoteCount != 2 || resp.Summary.TotalWeightedEngagement != 42.0 {
		t.Fatalf("unexpected summary totals: %+v", resp.Summary)
	}
	if resp.Summary.Comparison.NoteCountDelta != 1 {
		t.Fatalf("unexpected comparison delta: %+v", resp.Summary.Comparison)
	}
}

func TestGetAuthorAnalyticsRecycleCandidates_AppliesDefaults(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorRecycleCandidatesFn: func(
			_ context.Context,
			pubkey string,
			windowDays int,
			minAgeDays int,
			minPerformancePercentile float64,
			includeReplies bool,
			excludeRecentlyReposted bool,
			recentRepostWindowDays int,
			limit int,
		) ([]store.AuthorRecycleCandidateProjection, error) {
			if pubkey != "pubkey_x" ||
				windowDays != 90 ||
				minAgeDays != 30 ||
				limit != 20 ||
				minPerformancePercentile != 70 ||
				includeReplies ||
				!excludeRecentlyReposted ||
				recentRepostWindowDays != 30 {
				t.Fatalf(
					"unexpected args: pubkey=%s window=%d minAge=%d limit=%d percentile=%v includeReplies=%t excludeRecent=%t recentRepostWindow=%d",
					pubkey, windowDays, minAgeDays, limit, minPerformancePercentile, includeReplies, excludeRecentlyReposted, recentRepostWindowDays,
				)
			}
			return []store.AuthorRecycleCandidateProjection{
				{
					EventID:               "n1",
					WeightedEngagement:    20,
					PerformancePercentile: 100,
				},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/recycle-candidates", handlers.GetAuthorAnalyticsRecycleCandidates)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/authors/pubkey_x/analytics/recycle-candidates", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Pubkey  string `json:"pubkey"`
		Filters struct {
			Window string `json:"window"`
			MinAge string `json:"min_age"`
		} `json:"filters"`
		Items []struct {
			EventID               string  `json:"event_id"`
			PerformancePercentile float64 `json:"performance_percentile"`
		} `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pubkey != "pubkey_x" ||
		resp.Filters.Window != "90d" ||
		resp.Filters.MinAge != "30d" ||
		len(resp.Items) != 1 ||
		resp.Items[0].EventID != "n1" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestGetAuthorAnalyticsRecycleCandidates_ValidatesFilters(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/authors/{pubkey}/analytics/recycle-candidates", handlers.GetAuthorAnalyticsRecycleCandidates)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/authors/pubkey_x/analytics/recycle-candidates?window=30d&min_age=30d&min_performance_percentile=150",
		nil,
	)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
	}
}
