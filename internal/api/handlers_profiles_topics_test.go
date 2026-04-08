package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetProfileTopics_WindowBehaviorAndPayload(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorTopicStatsFn: func(_ context.Context, pubkey string, windowDays int, limit int) ([]store.AuthorTopicStatsProjection, error) {
			if pubkey != "pk_1" || windowDays != 7 || limit != 5 {
				t.Fatalf("unexpected args: pubkey=%s window=%d limit=%d", pubkey, windowDays, limit)
			}
			return []store.AuthorTopicStatsProjection{
				{Hashtag: "nostr", UsageCount: 4, ActiveDays: 2},
			}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/profiles/{pubkey}/topics", handlers.GetProfileTopics)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/profiles/pk_1/topics?window=7d&limit=5", nil)
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
	if resp.Pubkey != "pk_1" || resp.Window != "7d" || len(resp.Items) != 1 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Items[0].Hashtag != "nostr" || resp.Items[0].UsageCount != 4 || resp.Items[0].ActiveDays != 2 {
		t.Fatalf("unexpected topic row: %+v", resp.Items[0])
	}
}

func TestGetProfileTopics_SparseProfileReturnsEmptyItems(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getAuthorTopicStatsFn: func(_ context.Context, pubkey string, windowDays int, limit int) ([]store.AuthorTopicStatsProjection, error) {
			if pubkey != "pk_sparse" || windowDays != 30 {
				t.Fatalf("unexpected args: pubkey=%s window=%d limit=%d", pubkey, windowDays, limit)
			}
			return []store.AuthorTopicStatsProjection{}, nil
		},
	}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/profiles/{pubkey}/topics", handlers.GetProfileTopics)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/profiles/pk_sparse/topics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Window string `json:"window"`
		Items  []any  `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Window != "30d" {
		t.Fatalf("unexpected default window: %q", resp.Window)
	}
	if len(resp.Items) != 0 {
		t.Fatalf("expected empty topic items for sparse profile, got %+v", resp.Items)
	}
}

func TestGetProfileTopics_ValidatesWindow(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/profiles/{pubkey}/topics", handlers.GetProfileTopics)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/profiles/pk_1/topics?window=2d", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
	}
}
