package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetTrustScore_SuccessAndNotFound(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
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
	h := mustNewHandlers(t, fakeEventReader{
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

func TestGetTrustScore_BadRequestAndInternalError(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrustScoreFn: func(_ context.Context, pubkey string) (store.TrustGlobalScore, error) {
			if pubkey != "boom" {
				t.Fatalf("unexpected pubkey: %q", pubkey)
			}
			return store.TrustGlobalScore{}, errors.New("store unavailable")
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/trust/scores/{pubkey}", h.GetTrustScore)

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores/blank", nil)
	badReq.SetPathValue("pubkey", " ")
	badRec := httptest.NewRecorder()
	h.GetTrustScore(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for empty pubkey: got %d want %d", badRec.Code, http.StatusBadRequest)
	}

	errReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores/boom", nil)
	errRec := httptest.NewRecorder()
	mux.ServeHTTP(errRec, errReq)
	if errRec.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status for internal error: got %d want %d", errRec.Code, http.StatusInternalServerError)
	}
}

func TestListTopTrustScores_DefaultLimitAndInternalError(t *testing.T) {
	callCount := 0
	h := mustNewHandlers(t, fakeEventReader{
		listTopTrustFn: func(_ context.Context, limit int) ([]store.TrustGlobalScore, error) {
			callCount++
			switch callCount {
			case 1:
				if limit != 50 {
					t.Fatalf("expected default limit 50, got %d", limit)
				}
				return []store.TrustGlobalScore{{Pubkey: "a", Score: 10, Rank: 1}}, nil
			case 2:
				if limit != 1 {
					t.Fatalf("expected explicit limit 1, got %d", limit)
				}
				return nil, errors.New("store unavailable")
			default:
				t.Fatalf("unexpected call count: %d", callCount)
				return nil, nil
			}
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/trust/scores", h.ListTopTrustScores)

	defaultReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores", nil)
	defaultRec := httptest.NewRecorder()
	mux.ServeHTTP(defaultRec, defaultReq)
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for default limit request: got %d want %d", defaultRec.Code, http.StatusOK)
	}

	errReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores?limit=1", nil)
	errRec := httptest.NewRecorder()
	mux.ServeHTTP(errRec, errReq)
	if errRec.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status for internal error: got %d want %d", errRec.Code, http.StatusInternalServerError)
	}
}

func TestTrustHandlers_ReturnNotImplementedForUnsupportedCapability(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrustScoreFn: func(_ context.Context, pubkey string) (store.TrustGlobalScore, error) {
			return store.TrustGlobalScore{}, errors.Join(query.ErrUnsupportedCapability, errors.New("query: trust score unsupported"))
		},
		listTopTrustFn: func(_ context.Context, limit int) ([]store.TrustGlobalScore, error) {
			return nil, errors.Join(query.ErrUnsupportedCapability, errors.New("query: top trusted pubkeys unsupported"))
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/trust/scores/{pubkey}", h.GetTrustScore)
	mux.HandleFunc("GET /api/v1/trust/scores", h.ListTopTrustScores)

	scoreReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores/abc", nil)
	scoreRec := httptest.NewRecorder()
	mux.ServeHTTP(scoreRec, scoreReq)
	if scoreRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status: got %d want %d", scoreRec.Code, http.StatusNotImplemented)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/trust/scores?limit=2", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status: got %d want %d", listRec.Code, http.StatusNotImplemented)
	}
}
