package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
)

func makeSearchNotes(count int, startCreatedAt int64) []json.RawMessage {
	out := make([]json.RawMessage, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, json.RawMessage(fmt.Sprintf(
			`{"id":"note_%d","created_at":%d}`, i, startCreatedAt-int64(i),
		)))
	}
	return out
}

func TestSearchNotes_EmitsNextCursorAndResumesOffset(t *testing.T) {
	var gotOffsets []int
	h := mustNewHandlers(t, fakeEventReader{
		searchNotesFn: func(_ context.Context, q string, sort string, _ *time.Duration, _ string, limit int, offset int) ([]json.RawMessage, error) {
			if q != "nostr" || sort != "relevant" {
				t.Fatalf("unexpected search args: q=%q sort=%q", q, sort)
			}
			gotOffsets = append(gotOffsets, offset)
			return makeSearchNotes(limit, 5000), nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/search/notes", h.SearchNotes)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/search/notes?q=nostr&limit=2", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	var firstPage struct {
		Notes      []json.RawMessage `json:"notes"`
		NextCursor string            `json:"next_cursor"`
		Offset     int               `json:"offset"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(firstPage.Notes) != 2 || firstPage.NextCursor == "" {
		t.Fatalf("expected full first page with next_cursor, got %+v", firstPage)
	}

	req2 := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/search/notes?q=nostr&limit=2&cursor="+url.QueryEscape(firstPage.NextCursor),
		nil,
	)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("unexpected continuation status: %d body=%s", rec2.Code, rec2.Body.String())
	}
	var secondPage struct {
		Offset     int    `json:"offset"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if secondPage.Offset != 2 {
		t.Fatalf("expected cursor to resume at offset 2, got %d", secondPage.Offset)
	}
	if len(gotOffsets) != 2 || gotOffsets[1] != 2 {
		t.Fatalf("unexpected reader offsets: %v", gotOffsets)
	}
	if secondPage.NextCursor == "" {
		t.Fatalf("expected chained next_cursor on full second page")
	}
}

func TestSearchNotes_LatestCursorCarriesKeysetPosition(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		searchNotesFn: func(_ context.Context, _ string, sort string, _ *time.Duration, _ string, limit int, _ int) ([]json.RawMessage, error) {
			if sort != "latest" {
				t.Fatalf("unexpected sort: %q", sort)
			}
			return makeSearchNotes(limit, 9000), nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/search/notes", h.SearchNotes)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/search/notes?q=nostr&sort=latest&limit=2", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	if page.NextCursor == "" {
		t.Fatalf("expected next_cursor for full latest page")
	}
	scope := searchCursorScopeHash("notes", "nostr", "latest", "", "")
	decoded, err := decodeSearchCursor(page.NextCursor, scope)
	if err != nil {
		t.Fatalf("decode emitted cursor: %v", err)
	}
	// Last of the two fake notes: note_1 with created_at 8999.
	if decoded.CreatedAt != 8999 || decoded.ID != "note_1" || decoded.Offset != 2 {
		t.Fatalf("unexpected keyset cursor payload: %+v", decoded)
	}
}

func TestSearchNotes_PartialPageOmitsNextCursor(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		searchNotesFn: func(_ context.Context, _ string, _ string, _ *time.Duration, _ string, _ int, _ int) ([]json.RawMessage, error) {
			return makeSearchNotes(1, 100), nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/search/notes", h.SearchNotes)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/search/notes?q=nostr&limit=5", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, exists := body["next_cursor"]; exists {
		t.Fatalf("partial page must not emit next_cursor: %v", body)
	}
}

func TestSearchNotes_RejectsCursorWithOffsetAndScopeMismatch(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		searchNotesFn: func(_ context.Context, _ string, _ string, _ *time.Duration, _ string, limit int, _ int) ([]json.RawMessage, error) {
			return makeSearchNotes(limit, 100), nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/search/notes", h.SearchNotes)

	scope := searchCursorScopeHash("notes", "nostr", "relevant", "", "")
	cursor, err := encodeSearchCursor(searchCursorPayload{QHash: scope, Sort: "relevant", Offset: 20})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	// cursor + offset must be rejected.
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/search/notes?q=nostr&offset=10&cursor="+url.QueryEscape(cursor),
		nil,
	)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for cursor+offset, got %d", rec.Code)
	}

	// Cursor issued for a different query must be rejected.
	req2 := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/search/notes?q=bitcoin&cursor="+url.QueryEscape(cursor),
		nil,
	)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for scope mismatch, got %d", rec2.Code)
	}
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	if payload.Error.Code != "invalid_cursor" {
		t.Fatalf("unexpected error code: %q", payload.Error.Code)
	}
}

func TestSearchProfiles_EmitsAndResumesCursor(t *testing.T) {
	var gotOffsets []int
	h := mustNewHandlers(t, fakeEventReader{
		searchProfilesWithOptionsFn: func(_ context.Context, q string, sort string, limit int, offset int) ([]store.ProfileProjection, error) {
			if q != "alice" || sort != "relevant" {
				t.Fatalf("unexpected profile search args: q=%q sort=%q", q, sort)
			}
			gotOffsets = append(gotOffsets, offset)
			out := make([]store.ProfileProjection, 0, limit)
			for i := 0; i < limit; i++ {
				out = append(out, store.ProfileProjection{
					Pubkey:            fmt.Sprintf("pk_%d_%d", offset, i),
					MetadataEventID:   "meta",
					MetadataCreatedAt: 1710000000,
					ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
				})
			}
			return out, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/search/profiles", h.SearchProfiles)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/search/profiles?q=alice&limit=2", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	var firstPage struct {
		Profiles   []json.RawMessage `json:"profiles"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(firstPage.Profiles) != 2 || firstPage.NextCursor == "" {
		t.Fatalf("expected full profile page with next_cursor, got %+v", firstPage)
	}

	req2 := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/search/profiles?q=alice&limit=2&cursor="+url.QueryEscape(firstPage.NextCursor),
		nil,
	)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("unexpected continuation status: %d", rec2.Code)
	}
	var secondPage struct {
		Offset int `json:"offset"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if secondPage.Offset != 2 {
		t.Fatalf("expected profile cursor to resume at offset 2, got %d", secondPage.Offset)
	}
	if len(gotOffsets) != 2 || gotOffsets[1] != 2 {
		t.Fatalf("unexpected reader offsets: %v", gotOffsets)
	}
}
