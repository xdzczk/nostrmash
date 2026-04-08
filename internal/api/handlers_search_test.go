package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestSearchDedicatedRoutes_Success(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		searchNotesFn: func(_ context.Context, q string, sort string, window *time.Duration, limit int, offset int) ([]json.RawMessage, error) {
			if q != "nostr" {
				t.Fatalf("unexpected notes query: %q", q)
			}
			if sort != "latest" {
				t.Fatalf("unexpected notes sort: %q", sort)
			}
			if window == nil || *window != 7*24*time.Hour {
				t.Fatalf("unexpected notes window: %#v", window)
			}
			if limit != 2 || offset != 1 {
				t.Fatalf("unexpected notes pagination: limit=%d offset=%d", limit, offset)
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"note_1"}`),
				json.RawMessage(`{"id":"note_2"}`),
			}, nil
		},
		searchProfilesWithOptionsFn: func(_ context.Context, q string, sort string, limit int, offset int) ([]store.ProfileProjection, error) {
			if q != "alice" {
				t.Fatalf("unexpected profiles query: %q", q)
			}
			if sort != "relevant" {
				t.Fatalf("unexpected profiles sort: %q", sort)
			}
			if limit != 2 || offset != 3 {
				t.Fatalf("unexpected profiles pagination: limit=%d offset=%d", limit, offset)
			}
			return []store.ProfileProjection{
				{
					Pubkey:            "pk_alice",
					MetadataEventID:   "meta_1",
					MetadataCreatedAt: 1710000000,
					ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
				},
			}, nil
		},
		suggestProfilesFn: func(_ context.Context, q string, limit int) ([]store.ProfileProjection, error) {
			if q != "nost" {
				t.Fatalf("unexpected suggest profile query: %q", q)
			}
			if limit != 2 {
				t.Fatalf("unexpected suggest profile limit: %d", limit)
			}
			return []store.ProfileProjection{
				{
					Pubkey:            "pk_suggest",
					MetadataEventID:   "meta_suggest",
					MetadataCreatedAt: 1710001000,
					ProfileJSON:       json.RawMessage(`{"name":"nostr fan"}`),
				},
			}, nil
		},
		suggestHashtagsFn: func(_ context.Context, q string, limit int) ([]store.TrendingHashtag, error) {
			if q != "nost" {
				t.Fatalf("unexpected suggest hashtag query: %q", q)
			}
			if limit != 2 {
				t.Fatalf("unexpected suggest hashtag limit: %d", limit)
			}
			return []store.TrendingHashtag{
				{Hashtag: "nostr", EventCount: 12, UniqueAuthors: 9},
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/search/notes", h.SearchNotes)
	mux.HandleFunc("GET /api/v1/search/profiles", h.SearchProfiles)
	mux.HandleFunc("GET /api/v1/search/suggest", h.SearchSuggest)

	notesReq := httptest.NewRequest(http.MethodGet, "/api/v1/search/notes?q=nostr&sort=latest&window=7d&limit=2&offset=1", nil)
	notesRec := httptest.NewRecorder()
	mux.ServeHTTP(notesRec, notesReq)
	if notesRec.Code != http.StatusOK {
		t.Fatalf("unexpected notes status: got %d want %d", notesRec.Code, http.StatusOK)
	}
	var notesBody struct {
		Query        string            `json:"query"`
		Sort         string            `json:"sort"`
		Window       string            `json:"window"`
		Notes        []json.RawMessage `json:"notes"`
		Limit        int               `json:"limit"`
		Offset       int               `json:"offset"`
		TrustMode    string            `json:"trust_mode"`
		TrustApplied bool              `json:"trust_applied"`
		ResultScope  string            `json:"result_scope"`
	}
	if err := json.Unmarshal(notesRec.Body.Bytes(), &notesBody); err != nil {
		t.Fatalf("decode notes response: %v", err)
	}
	if notesBody.Query != "nostr" || notesBody.Sort != "latest" || notesBody.Window != "7d" {
		t.Fatalf("unexpected notes contract values: %#v", notesBody)
	}
	if len(notesBody.Notes) != 2 || notesBody.Limit != 2 || notesBody.Offset != 1 {
		t.Fatalf("unexpected notes payload: %#v", notesBody)
	}
	if notesBody.TrustMode != "open" || notesBody.TrustApplied || notesBody.ResultScope != "open" {
		t.Fatalf("unexpected notes trust metadata: %#v", notesBody)
	}

	profilesReq := httptest.NewRequest(http.MethodGet, "/api/v1/search/profiles?q=alice&limit=2&offset=3", nil)
	profilesRec := httptest.NewRecorder()
	mux.ServeHTTP(profilesRec, profilesReq)
	if profilesRec.Code != http.StatusOK {
		t.Fatalf("unexpected profiles status: got %d want %d", profilesRec.Code, http.StatusOK)
	}
	var profilesBody struct {
		Query    string `json:"query"`
		Sort     string `json:"sort"`
		Profiles []struct {
			Pubkey string `json:"pubkey"`
		} `json:"profiles"`
		Limit        int    `json:"limit"`
		Offset       int    `json:"offset"`
		TrustMode    string `json:"trust_mode"`
		TrustApplied bool   `json:"trust_applied"`
		ResultScope  string `json:"result_scope"`
	}
	if err := json.Unmarshal(profilesRec.Body.Bytes(), &profilesBody); err != nil {
		t.Fatalf("decode profiles response: %v", err)
	}
	if profilesBody.Query != "alice" || profilesBody.Sort != "relevant" {
		t.Fatalf("unexpected profiles contract values: %#v", profilesBody)
	}
	if len(profilesBody.Profiles) != 1 || profilesBody.Profiles[0].Pubkey != "pk_alice" {
		t.Fatalf("unexpected profiles payload: %#v", profilesBody.Profiles)
	}
	if profilesBody.Limit != 2 || profilesBody.Offset != 3 {
		t.Fatalf("unexpected profiles pagination in response: %#v", profilesBody)
	}
	if profilesBody.TrustMode != "prefer_trusted" || !profilesBody.TrustApplied || profilesBody.ResultScope != "prefer_trusted" {
		t.Fatalf("unexpected profiles trust metadata: %#v", profilesBody)
	}

	suggestReq := httptest.NewRequest(http.MethodGet, "/api/v1/search/suggest?q=nost&limit=2", nil)
	suggestRec := httptest.NewRecorder()
	mux.ServeHTTP(suggestRec, suggestReq)
	if suggestRec.Code != http.StatusOK {
		t.Fatalf("unexpected suggest status: got %d want %d", suggestRec.Code, http.StatusOK)
	}
	var suggestBody struct {
		Query    string `json:"query"`
		Profiles []struct {
			Pubkey string `json:"pubkey"`
		} `json:"profiles"`
		Hashtags []struct {
			Hashtag string `json:"hashtag"`
		} `json:"hashtags"`
	}
	if err := json.Unmarshal(suggestRec.Body.Bytes(), &suggestBody); err != nil {
		t.Fatalf("decode suggest response: %v", err)
	}
	if suggestBody.Query != "nost" {
		t.Fatalf("unexpected suggest query echo: %#v", suggestBody)
	}
	if len(suggestBody.Profiles) != 1 || suggestBody.Profiles[0].Pubkey != "pk_suggest" {
		t.Fatalf("unexpected suggest profile payload: %#v", suggestBody.Profiles)
	}
	if len(suggestBody.Hashtags) != 1 || suggestBody.Hashtags[0].Hashtag != "nostr" {
		t.Fatalf("unexpected suggest hashtag payload: %#v", suggestBody.Hashtags)
	}
}

func TestSearchDedicatedRoutes_Validation(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/search/notes", h.SearchNotes)
	mux.HandleFunc("GET /api/v1/search/profiles", h.SearchProfiles)
	mux.HandleFunc("GET /api/v1/search/suggest", h.SearchSuggest)

	cases := []string{
		"/api/v1/search/notes?q=",
		"/api/v1/search/notes?q=ok&sort=top",
		"/api/v1/search/notes?q=ok&window=30d",
		"/api/v1/search/notes?q=ok&offset=-1",
		"/api/v1/search/profiles?q=",
		"/api/v1/search/profiles?q=ok&sort=latest",
		"/api/v1/search/suggest?q=ok&limit=0",
	}
	for _, path := range cases {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status for %s: got %d want %d", path, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestSearchSuggest_EmptyAndShortQueryReturnsEmpty(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/search/suggest", h.SearchSuggest)
	cases := []string{
		"/api/v1/search/suggest?q=",
		"/api/v1/search/suggest?q=n",
		"/api/v1/search/suggest?q=%23",
	}
	for _, path := range cases {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status for %s: got %d want %d", path, rec.Code, http.StatusOK)
		}
		var body struct {
			Profiles []any `json:"profiles"`
			Hashtags []any `json:"hashtags"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode suggest empty response: %v", err)
		}
		if len(body.Profiles) != 0 || len(body.Hashtags) != 0 {
			t.Fatalf("expected empty suggestions for %s, got %#v", path, body)
		}
	}
}

func TestSearchCombinedRoute_RemainsIntact(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		searchEventsFn: func(_ context.Context, q string, limit int) ([]json.RawMessage, error) {
			if q != "nostr" || limit != 1 {
				t.Fatalf("unexpected combined events args: q=%q limit=%d", q, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_1"}`)}, nil
		},
		searchProfilesFn: func(_ context.Context, q string, limit int) ([]store.ProfileProjection, error) {
			if q != "nostr" || limit != 1 {
				t.Fatalf("unexpected combined profiles args: q=%q limit=%d", q, limit)
			}
			return []store.ProfileProjection{
				{Pubkey: "pk_1", ProfileJSON: json.RawMessage(`{"name":"one"}`)},
			}, nil
		},
	}, 200)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/search?q=nostr&limit=1", nil)
	rec := httptest.NewRecorder()
	http.HandlerFunc(h.Search).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode combined response: %v", err)
	}
	if _, ok := body["events"]; !ok {
		t.Fatalf("combined response missing events: %#v", body)
	}
	if _, ok := body["profiles"]; !ok {
		t.Fatalf("combined response missing profiles: %#v", body)
	}
	if body["trust_mode"] != "prefer_trusted" || body["trust_applied"] != true || body["result_scope"] != "prefer_trusted" {
		t.Fatalf("unexpected combined trust metadata: %#v", body)
	}
}

func TestSearchProfiles_TrustMetadataByMode(t *testing.T) {
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
					searchProfilesWithOptionsFn: func(_ context.Context, _ string, _ string, _ int, _ int) ([]store.ProfileProjection, error) {
						return []store.ProfileProjection{
							{Pubkey: "pk_trusted", ProfileJSON: json.RawMessage(`{"name":"trusted"}`)},
							{Pubkey: "pk_open", ProfileJSON: json.RawMessage(`{"name":"open"}`)},
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
					SearchRankingTrustMode: tc.mode,
				},
			})

			req := httptest.NewRequest(http.MethodGet, "/api/v1/search/profiles?q=alice&limit=2", nil)
			rec := httptest.NewRecorder()
			http.HandlerFunc(h.SearchProfiles).ServeHTTP(rec, req)
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
