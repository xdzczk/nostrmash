package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestKindExpansionHTTPRoutes(t *testing.T) {
	t.Parallel()
	picture := json.RawMessage(`{"id":"pic1","kind":20}`)
	live := json.RawMessage(`{"id":"live1","kind":30311}`)
	comment := json.RawMessage(`{"id":"c1","kind":1111}`)
	report := store.ReportTarget{
		ReportEventID:  "r1",
		ReporterPubkey: "reporter",
		ReportType:     "spam",
		Content:        "reason",
		CreatedAt:      10,
		TargetEventID:  "pic1",
	}
	userReport := report
	userReport.TargetEventID = ""
	userReport.TargetPubkey = "pk1"
	handlers := mustNewHandlers(t, fakeEventReader{
		kindExpansionHooks: kindExpansionHooks{
			authorMedia: func(context.Context, string, int) ([]json.RawMessage, error) {
				return []json.RawMessage{picture}, nil
			},
			authorLive: func(context.Context, string, int) ([]json.RawMessage, error) {
				return []json.RawMessage{live}, nil
			},
			authorPins: func(context.Context, string) ([]store.PinItem, error) {
				return []store.PinItem{{Tag: "e", Value: "pic1"}}, nil
			},
			liveActivities: func(_ context.Context, status, followedBy string, _ int) ([]json.RawMessage, error) {
				if status != "live" || followedBy != "" {
					t.Fatalf("live query status=%q followedBy=%q", status, followedBy)
				}
				return []json.RawMessage{live}, nil
			},
			eventComments: func(context.Context, string, int) ([]json.RawMessage, error) {
				return []json.RawMessage{comment}, nil
			},
			eventReports: func(context.Context, string, int) ([]store.ReportTarget, error) {
				return []store.ReportTarget{report}, nil
			},
			userReports: func(context.Context, string, int) ([]store.ReportTarget, error) {
				return []store.ReportTarget{userReport}, nil
			},
		},
	}, 10)

	checks := []struct {
		path   string
		params map[string]string
		needle string
	}{
		{"/api/v1/users/pk1/media", map[string]string{"pubkey": "pk1"}, "pic1"},
		{"/api/v1/users/pk1/live", map[string]string{"pubkey": "pk1"}, "live1"},
		{"/api/v1/users/pk1/pins", map[string]string{"pubkey": "pk1"}, "pic1"},
		{"/api/v1/live", nil, "live1"},
		{"/api/v1/events/pic1/comments", map[string]string{"id": "pic1"}, "c1"},
		{"/api/v1/events/pic1/reports", map[string]string{"id": "pic1"}, "spam"},
		{"/api/v1/users/pk1/reports", map[string]string{"pubkey": "pk1"}, "pk1"},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/users/{pubkey}/media", handlers.GetMedia)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/live", handlers.GetUserLive)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/pins", handlers.GetPins)
	mux.HandleFunc("GET /api/v1/live", handlers.GetLive)
	mux.HandleFunc("GET /api/v1/events/{id}/comments", handlers.GetEventComments)
	mux.HandleFunc("GET /api/v1/events/{id}/reports", handlers.GetEventReports)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/reports", handlers.GetUserReports)
	for _, check := range checks {
		req := httptest.NewRequest(http.MethodGet, check.path, nil)
		for key, value := range check.params {
			req.SetPathValue(key, value)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status %d body %s", check.path, rec.Code, rec.Body.String())
		}
		if !json.Valid(rec.Body.Bytes()) || !strings.Contains(rec.Body.String(), check.needle) {
			t.Fatalf("%s body %s", check.path, rec.Body.String())
		}
	}

	bad := httptest.NewRequest(http.MethodGet, "/api/v1/live?status=nope", nil)
	rec := httptest.NewRecorder()
	handlers.GetLive(rec, bad)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad status code %d", rec.Code)
	}
}
