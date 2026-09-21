package query

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// keysetSearchFakeReader implements both the offset and keyset notes-search
// capabilities so tests can observe which path the service selects.
type keysetSearchFakeReader struct {
	fakeReader
	searchNotesFn       func(ctx context.Context, query string, sort string, window *time.Duration, language string, limit int, offset int) ([]json.RawMessage, error)
	searchNotesBeforeFn func(ctx context.Context, query string, window *time.Duration, language string, limit int, beforeCreatedAt int64, beforeID string) ([]json.RawMessage, error)
}

func (f keysetSearchFakeReader) SearchNotes(ctx context.Context, query string, sort string, window *time.Duration, language string, limit int, offset int) ([]json.RawMessage, error) {
	if f.searchNotesFn == nil {
		return []json.RawMessage{}, nil
	}
	return f.searchNotesFn(ctx, query, sort, window, language, limit, offset)
}

func (f keysetSearchFakeReader) SearchNotesBefore(ctx context.Context, query string, window *time.Duration, language string, limit int, beforeCreatedAt int64, beforeID string) ([]json.RawMessage, error) {
	if f.searchNotesBeforeFn == nil {
		return []json.RawMessage{}, nil
	}
	return f.searchNotesBeforeFn(ctx, query, window, language, limit, beforeCreatedAt, beforeID)
}

// offsetOnlySearchFakeReader has no keyset capability.
type offsetOnlySearchFakeReader struct {
	fakeReader
	searchNotesFn func(ctx context.Context, query string, sort string, window *time.Duration, language string, limit int, offset int) ([]json.RawMessage, error)
}

func (f offsetOnlySearchFakeReader) SearchNotes(ctx context.Context, query string, sort string, window *time.Duration, language string, limit int, offset int) ([]json.RawMessage, error) {
	if f.searchNotesFn == nil {
		return []json.RawMessage{}, nil
	}
	return f.searchNotesFn(ctx, query, sort, window, language, limit, offset)
}

func TestSearchNotes_LatestWithKeysetPrefersKeysetReader(t *testing.T) {
	t.Parallel()
	keysetCalled := false
	svc := mustNewService(t, keysetSearchFakeReader{
		searchNotesFn: func(context.Context, string, string, *time.Duration, string, int, int) ([]json.RawMessage, error) {
			t.Errorf("offset search must not run when a keyset position is present")
			return nil, nil
		},
		searchNotesBeforeFn: func(_ context.Context, query string, _ *time.Duration, _ string, limit int, beforeCreatedAt int64, beforeID string) ([]json.RawMessage, error) {
			keysetCalled = true
			if query != "nostr" || limit != 2 {
				t.Errorf("unexpected keyset search args: query=%q limit=%d", query, limit)
			}
			if beforeCreatedAt != 900 || beforeID != "evt_cursor" {
				t.Errorf("unexpected keyset position: created_at=%d id=%q", beforeCreatedAt, beforeID)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_older","created_at":800}`)}, nil
		},
	})

	rows, err := svc.SearchNotes(context.Background(), NotesSearchParams{
		Query:           "nostr",
		Limit:           2,
		Sort:            "latest",
		Offset:          20,
		BeforeCreatedAt: 900,
		BeforeID:        "evt_cursor",
	})
	if err != nil {
		t.Fatalf("SearchNotes: %v", err)
	}
	if !keysetCalled {
		t.Fatalf("expected keyset search path to run")
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected result count: %d", len(rows))
	}
}

func TestSearchNotes_LatestWithKeysetFallsBackToOffsetReader(t *testing.T) {
	t.Parallel()
	offsetCalled := false
	svc := mustNewService(t, offsetOnlySearchFakeReader{
		searchNotesFn: func(_ context.Context, _ string, sort string, _ *time.Duration, _ string, _ int, offset int) ([]json.RawMessage, error) {
			offsetCalled = true
			if sort != "latest" || offset != 20 {
				t.Errorf("unexpected offset fallback args: sort=%q offset=%d", sort, offset)
			}
			return []json.RawMessage{}, nil
		},
	})

	if _, err := svc.SearchNotes(context.Background(), NotesSearchParams{
		Query:           "nostr",
		Limit:           2,
		Sort:            "latest",
		Offset:          20,
		BeforeCreatedAt: 900,
		BeforeID:        "evt_cursor",
	}); err != nil {
		t.Fatalf("SearchNotes: %v", err)
	}
	if !offsetCalled {
		t.Fatalf("expected offset search fallback to run for keyset-incapable readers")
	}
}
