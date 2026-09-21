package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	storeread "github.com/xdzczk/nostrmash/internal/store/read"
)

func hashtagNotesTestMux(t *testing.T, notesFn func(ctx context.Context, hashtag, sort, window string, limit, offset int) ([]storeread.TrendingNote, error)) *http.ServeMux {
	t.Helper()
	h := mustNewHandlers(t, fakeEventReader{getHashtagNotesFn: notesFn}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}/notes", h.GetHashtagNotes)
	return mux
}

func makeTrendingNotes(count, startAt int) []storeread.TrendingNote {
	notes := make([]storeread.TrendingNote, 0, count)
	for i := 0; i < count; i++ {
		notes = append(notes, storeread.TrendingNote{
			EventID:      fmt.Sprintf("note_%d", startAt+i),
			AuthorPubkey: "pk_1",
			CreatedAt:    int64(1_700_000_000 - startAt - i),
			Content:      "hello",
		})
	}
	return notes
}

func TestGetHashtagNotes_EmitsNextCursorAndResumesFromIt(t *testing.T) {
	var offsets []int
	mux := hashtagNotesTestMux(t, func(_ context.Context, hashtag, sort, window string, limit, offset int) ([]storeread.TrendingNote, error) {
		if hashtag != "nostr" || sort != "latest" || window != "24h" || limit != 2 {
			t.Fatalf("unexpected args: hashtag=%s sort=%s window=%s limit=%d", hashtag, sort, window, limit)
		}
		offsets = append(offsets, offset)
		if offset == 0 {
			return makeTrendingNotes(2, 0), nil
		}
		return makeTrendingNotes(1, offset), nil
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/nostr/notes?limit=2", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var firstPage struct {
		Notes      []json.RawMessage `json:"notes"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(firstPage.Notes) != 2 {
		t.Fatalf("unexpected first page size: %d", len(firstPage.Notes))
	}
	if firstPage.NextCursor == "" {
		t.Fatalf("expected next_cursor on full first page")
	}

	req2 := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/discovery/hashtags/nostr/notes?limit=2&cursor="+url.QueryEscape(firstPage.NextCursor),
		nil,
	)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("unexpected continuation status: got %d body=%s", rec2.Code, rec2.Body.String())
	}
	var secondPage struct {
		Notes      []json.RawMessage `json:"notes"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.NewDecoder(rec2.Body).Decode(&secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(secondPage.Notes) != 1 {
		t.Fatalf("unexpected second page size: %d", len(secondPage.Notes))
	}
	if secondPage.NextCursor != "" {
		t.Fatalf("expected no next_cursor on partial page, got %q", secondPage.NextCursor)
	}
	if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != 2 {
		t.Fatalf("unexpected offsets: %v", offsets)
	}
}

func TestGetHashtagNotes_RejectsCursorWithOffset(t *testing.T) {
	mux := hashtagNotesTestMux(t, func(_ context.Context, _, _, _ string, limit, _ int) ([]storeread.TrendingNote, error) {
		return makeTrendingNotes(limit, 0), nil
	})

	first := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/nostr/notes?limit=2", nil)
	firstRec := httptest.NewRecorder()
	mux.ServeHTTP(firstRec, first)
	var firstPage struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.NewDecoder(firstRec.Body).Decode(&firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if firstPage.NextCursor == "" {
		t.Fatalf("expected next_cursor")
	}

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/discovery/hashtags/nostr/notes?limit=2&offset=4&cursor="+url.QueryEscape(firstPage.NextCursor),
		nil,
	)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for cursor+offset, got %d", rec.Code)
	}
}

func TestGetHashtagNotes_RejectsMalformedAndMismatchedCursors(t *testing.T) {
	mux := hashtagNotesTestMux(t, func(_ context.Context, _, _, _ string, limit, _ int) ([]storeread.TrendingNote, error) {
		return makeTrendingNotes(limit, 0), nil
	})

	malformed := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/nostr/notes?cursor=%21%21not-base64", nil)
	malformedRec := httptest.NewRecorder()
	mux.ServeHTTP(malformedRec, malformed)
	if malformedRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed cursor, got %d", malformedRec.Code)
	}

	// Cursor issued for one hashtag/sort/window scope must not continue another.
	first := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/nostr/notes?limit=2", nil)
	firstRec := httptest.NewRecorder()
	mux.ServeHTTP(firstRec, first)
	var firstPage struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.NewDecoder(firstRec.Body).Decode(&firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if firstPage.NextCursor == "" {
		t.Fatalf("expected next_cursor")
	}

	for _, path := range []string{
		"/api/v1/discovery/hashtags/bitcoin/notes?limit=2&cursor=",
		"/api/v1/discovery/hashtags/nostr/notes?limit=2&sort=top&cursor=",
		"/api/v1/discovery/hashtags/nostr/notes?limit=2&window=7d&cursor=",
	} {
		req := httptest.NewRequest(http.MethodGet, path+url.QueryEscape(firstPage.NextCursor), nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for scope mismatch on %s, got %d", path, rec.Code)
		}
	}
}
