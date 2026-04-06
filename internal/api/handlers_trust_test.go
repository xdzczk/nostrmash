package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetTrustScore_SuccessAndNotFound(t *testing.T) {
	h := NewHandlers(fakeEventReader{
		getTrustScoreFn: func(_ context.Context, pubkey string) (store.TrustGlobalScore, error) {
			if pubkey == "missing" {
				return store.TrustGlobalScore{}, store.ErrNotFound
			}
			return store.TrustGlobalScore{Pubkey: pubkey, Score: 12.0, Rank: 3}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/trust/scores/{pubkey}", h.GetTrustScore)

	okReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores/abc", nil)
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", okRec.Code, http.StatusOK)
	}

	notFoundReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores/missing", nil)
	notFoundRec := httptest.NewRecorder()
	mux.ServeHTTP(notFoundRec, notFoundReq)
	if notFoundRec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d want %d", notFoundRec.Code, http.StatusNotFound)
	}
}

func TestListTopTrustScores_SuccessAndBadLimit(t *testing.T) {
	h := NewHandlers(fakeEventReader{
		listTopTrustFn: func(_ context.Context, limit int) ([]store.TrustGlobalScore, error) {
			if limit != 2 {
				return nil, errors.New("unexpected limit")
			}
			return []store.TrustGlobalScore{
				{Pubkey: "a", Score: 10, Rank: 1},
				{Pubkey: "b", Score: 9, Rank: 2},
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/trust/scores", h.ListTopTrustScores)

	okReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores?limit=2", nil)
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", okRec.Code, http.StatusOK)
	}

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores?limit=9999", nil)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", badRec.Code, http.StatusBadRequest)
	}
}
