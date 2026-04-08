package query

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestSearchNotes_AdvancedSortWindowAndPagination(t *testing.T) {
	svc := mustNewService(t, fakeReader{})
	var called bool
	svc.reader = readerWithAdvancedSearch{
		Reader: svc.reader,
		searchNotesFn: func(_ context.Context, q string, sort string, window *time.Duration, limit int, offset int) ([]json.RawMessage, error) {
			called = true
			if q != "nostr" || sort != "latest" {
				t.Fatalf("unexpected notes args: q=%q sort=%q", q, sort)
			}
			if window == nil || *window != 7*24*time.Hour {
				t.Fatalf("unexpected notes window: %#v", window)
			}
			if limit != 3 || offset != 2 {
				t.Fatalf("unexpected notes pagination: limit=%d offset=%d", limit, offset)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"note_1"}`)}, nil
		},
	}
	window := 7 * 24 * time.Hour
	out, err := svc.SearchNotes(context.Background(), NotesSearchParams{
		Query:  "nostr",
		Sort:   "latest",
		Window: &window,
		Limit:  3,
		Offset: 2,
	})
	if err != nil {
		t.Fatalf("SearchNotes returned error: %v", err)
	}
	if !called {
		t.Fatal("expected advanced notes reader to be called")
	}
	if len(out) != 1 {
		t.Fatalf("unexpected notes output length: %d", len(out))
	}
}

func TestSearchProfiles_AdvancedPagination(t *testing.T) {
	svc := mustNewService(t, fakeReader{})
	var called bool
	svc.reader = readerWithAdvancedSearch{
		Reader: svc.reader,
		searchProfilesFn: func(_ context.Context, q string, sort string, limit int, offset int) ([]Profile, error) {
			called = true
			if q != "alice" || sort != "relevant" {
				t.Fatalf("unexpected profiles args: q=%q sort=%q", q, sort)
			}
			if limit != 2 || offset != 5 {
				t.Fatalf("unexpected profiles pagination: limit=%d offset=%d", limit, offset)
			}
			return []Profile{{Pubkey: "pk_alice"}}, nil
		},
	}
	out, err := svc.SearchProfiles(context.Background(), ProfileSearchParams{
		Query:  "alice",
		Sort:   "relevant",
		Limit:  2,
		Offset: 5,
	})
	if err != nil {
		t.Fatalf("SearchProfiles returned error: %v", err)
	}
	if !called {
		t.Fatal("expected advanced profiles reader to be called")
	}
	if len(out) != 1 || out[0].Pubkey != "pk_alice" {
		t.Fatalf("unexpected profiles output: %#v", out)
	}
}

func TestSearchNotes_EmptyQueryReturnsEmpty(t *testing.T) {
	svc := mustNewService(t, fakeReader{})
	out, err := svc.SearchNotes(context.Background(), NotesSearchParams{
		Query: "   ",
	})
	if err != nil {
		t.Fatalf("SearchNotes returned error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty result for empty query, got %d", len(out))
	}
}

func TestSearchProfiles_RejectsUnsupportedSort(t *testing.T) {
	svc := mustNewService(t, fakeReader{})
	_, err := svc.SearchProfiles(context.Background(), ProfileSearchParams{
		Query: "nostr",
		Sort:  "latest",
	})
	if err == nil {
		t.Fatal("expected error for unsupported profile sort")
	}
}

func TestCombinedSearch_UsesSharedSearchMethods(t *testing.T) {
	svc := mustNewService(t, fakeReader{})
	eventsCalled := false
	profilesCalled := false
	svc.reader = readerWithSearchFallback{
		Reader: svc.reader,
		searchEventsFn: func(_ context.Context, q string, limit int) ([]json.RawMessage, error) {
			eventsCalled = true
			if q != "nostr" || limit != 1 {
				t.Fatalf("unexpected event search args: q=%q limit=%d", q, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"evt_1"}`)}, nil
		},
		searchProfilesFn: func(_ context.Context, q string, limit int) ([]Profile, error) {
			profilesCalled = true
			if q != "nostr" || limit != 1 {
				t.Fatalf("unexpected profile search args: q=%q limit=%d", q, limit)
			}
			return []Profile{{Pubkey: "pk_1"}}, nil
		},
	}
	out, err := svc.Search(context.Background(), "nostr", 1)
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if !eventsCalled || !profilesCalled {
		t.Fatalf("expected both search branches to run, events=%v profiles=%v", eventsCalled, profilesCalled)
	}
	if len(out.Events) != 1 || len(out.Profiles) != 1 {
		t.Fatalf("unexpected combined search output: %#v", out)
	}
}

func TestSearchSuggestions_UsesSuggestionsReader(t *testing.T) {
	svc := mustNewService(t, fakeReader{})
	var profilesCalled bool
	var hashtagsCalled bool
	svc.reader = readerWithSuggestionSearch{
		Reader: svc.reader,
		suggestProfilesFn: func(_ context.Context, q string, limit int) ([]Profile, error) {
			profilesCalled = true
			if q != "alice" || limit != 5 {
				t.Fatalf("unexpected profile suggestion args: q=%q limit=%d", q, limit)
			}
			return []Profile{{Pubkey: "pk_alice"}}, nil
		},
		suggestHashtagsFn: func(_ context.Context, q string, limit int) ([]HashtagSuggestion, error) {
			hashtagsCalled = true
			if q != "alice" || limit != 5 {
				t.Fatalf("unexpected hashtag suggestion args: q=%q limit=%d", q, limit)
			}
			return []HashtagSuggestion{{Hashtag: "alice", EventCount: 3, UniqueAuthors: 2}}, nil
		},
	}
	out, err := svc.SearchSuggestions(context.Background(), "alice", 5)
	if err != nil {
		t.Fatalf("SearchSuggestions returned error: %v", err)
	}
	if !profilesCalled || !hashtagsCalled {
		t.Fatalf("expected both suggestion branches to run, profiles=%v hashtags=%v", profilesCalled, hashtagsCalled)
	}
	if len(out.Profiles) != 1 || out.Profiles[0].Pubkey != "pk_alice" {
		t.Fatalf("unexpected profile suggestions: %#v", out.Profiles)
	}
	if len(out.Hashtags) != 1 || out.Hashtags[0].Hashtag != "alice" {
		t.Fatalf("unexpected hashtag suggestions: %#v", out.Hashtags)
	}
}

func TestSearchSuggestions_EmptyQueryReturnsEmpty(t *testing.T) {
	svc := mustNewService(t, fakeReader{})
	out, err := svc.SearchSuggestions(context.Background(), "   ", 5)
	if err != nil {
		t.Fatalf("SearchSuggestions returned error: %v", err)
	}
	if len(out.Profiles) != 0 || len(out.Hashtags) != 0 {
		t.Fatalf("expected empty suggestions, got %#v", out)
	}
}

func TestSearchNotes_TrustModePreferTrustedBoostsAndPaginates(t *testing.T) {
	base := []json.RawMessage{
		mustRawEvent(t, "n1", "u1"),
		mustRawEvent(t, "n2", "u2"),
		mustRawEvent(t, "n3", "u3"),
		mustRawEvent(t, "n4", "u4"),
		mustRawEvent(t, "n5", "u5"),
	}
	var trustCalls int
	reader := readerWithAdvancedSearch{
		Reader: fakeReader{},
		searchNotesFn: func(_ context.Context, _ string, _ string, _ *time.Duration, limit int, offset int) ([]json.RawMessage, error) {
			if offset >= len(base) {
				return []json.RawMessage{}, nil
			}
			end := offset + limit
			if end > len(base) {
				end = len(base)
			}
			return base[offset:end], nil
		},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ TrustQualificationPolicy) (map[string]TrustQualification, error) {
			trustCalls++
			out := map[string]TrustQualification{}
			for _, pubkey := range pubkeys {
				out[pubkey] = TrustQualification{Pubkey: pubkey, Trusted: pubkey == "u3" || pubkey == "u5"}
			}
			return out, nil
		},
	}
	svc := mustNewServiceWithOptions(t, reader, ServiceOptions{
		SearchRankingTrustMode: trustModePreferTrusted,
	})
	out, err := svc.SearchNotes(context.Background(), NotesSearchParams{
		Query:  "nostr",
		Sort:   "relevant",
		Limit:  2,
		Offset: 1,
	})
	if err != nil {
		t.Fatalf("SearchNotes returned error: %v", err)
	}
	if trustCalls == 0 {
		t.Fatalf("expected trust qualification to be used")
	}
	if len(out) != 2 {
		t.Fatalf("expected paginated notes, got %d", len(out))
	}
	if eventID(t, out[0]) != "n5" || eventID(t, out[1]) != "n1" {
		t.Fatalf("unexpected trust-ranked notes page: %#v", out)
	}
}

func TestSearchNotes_TrustModeTrustedOnlyCanReturnEmpty(t *testing.T) {
	reader := readerWithAdvancedSearch{
		Reader: fakeReader{},
		searchNotesFn: func(_ context.Context, _ string, _ string, _ *time.Duration, _ int, _ int) ([]json.RawMessage, error) {
			return []json.RawMessage{
				mustRawEvent(t, "n1", "u1"),
				mustRawEvent(t, "n2", "u2"),
			}, nil
		},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ TrustQualificationPolicy) (map[string]TrustQualification, error) {
			out := map[string]TrustQualification{}
			for _, pubkey := range pubkeys {
				out[pubkey] = TrustQualification{Pubkey: pubkey, Trusted: false}
			}
			return out, nil
		},
	}
	svc := mustNewServiceWithOptions(t, reader, ServiceOptions{
		SearchRankingTrustMode: trustModeTrustedOnly,
	})
	out, err := svc.SearchNotes(context.Background(), NotesSearchParams{
		Query: "nostr",
		Sort:  "relevant",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("SearchNotes returned error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty trusted-only notes, got %#v", out)
	}
}

func TestSearchProfiles_TrustModePreferTrustedBoostsAndPaginates(t *testing.T) {
	base := []Profile{
		{Pubkey: "p1"},
		{Pubkey: "p2"},
		{Pubkey: "p3"},
		{Pubkey: "p4"},
	}
	reader := readerWithAdvancedSearch{
		Reader: fakeReader{},
		searchProfilesFn: func(_ context.Context, _ string, _ string, limit int, offset int) ([]Profile, error) {
			if offset >= len(base) {
				return []Profile{}, nil
			}
			end := offset + limit
			if end > len(base) {
				end = len(base)
			}
			return base[offset:end], nil
		},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ TrustQualificationPolicy) (map[string]TrustQualification, error) {
			out := map[string]TrustQualification{}
			for _, pubkey := range pubkeys {
				out[pubkey] = TrustQualification{Pubkey: pubkey, Trusted: pubkey == "p3"}
			}
			return out, nil
		},
	}
	svc := mustNewServiceWithOptions(t, reader, ServiceOptions{
		SearchRankingTrustMode: trustModePreferTrusted,
	})
	out, err := svc.SearchProfiles(context.Background(), ProfileSearchParams{
		Query:  "alice",
		Sort:   "relevant",
		Limit:  2,
		Offset: 1,
	})
	if err != nil {
		t.Fatalf("SearchProfiles returned error: %v", err)
	}
	if len(out) != 2 || out[0].Pubkey != "p1" || out[1].Pubkey != "p2" {
		t.Fatalf("unexpected trust-ranked profiles page: %#v", out)
	}
}

func TestSearchProfiles_TrustModeTrustedOnlyFilters(t *testing.T) {
	reader := readerWithAdvancedSearch{
		Reader: fakeReader{},
		searchProfilesFn: func(_ context.Context, _ string, _ string, _ int, _ int) ([]Profile, error) {
			return []Profile{
				{Pubkey: "p1"},
				{Pubkey: "p2"},
				{Pubkey: "p3"},
			}, nil
		},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ TrustQualificationPolicy) (map[string]TrustQualification, error) {
			out := map[string]TrustQualification{}
			for _, pubkey := range pubkeys {
				out[pubkey] = TrustQualification{Pubkey: pubkey, Trusted: pubkey == "p2"}
			}
			return out, nil
		},
	}
	svc := mustNewServiceWithOptions(t, reader, ServiceOptions{
		SearchRankingTrustMode: trustModeTrustedOnly,
	})
	out, err := svc.SearchProfiles(context.Background(), ProfileSearchParams{
		Query: "alice",
		Sort:  "relevant",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("SearchProfiles returned error: %v", err)
	}
	if len(out) != 1 || out[0].Pubkey != "p2" {
		t.Fatalf("expected trusted-only profiles, got %#v", out)
	}
}

type readerWithAdvancedSearch struct {
	Reader
	searchNotesFn            func(context.Context, string, string, *time.Duration, int, int) ([]json.RawMessage, error)
	searchProfilesFn         func(context.Context, string, string, int, int) ([]Profile, error)
	getTrustQualificationsFn func(context.Context, []string, TrustQualificationPolicy) (map[string]TrustQualification, error)
	isTrustedAuthorFn        func(context.Context, string, TrustQualificationPolicy) (bool, error)
}

func (r readerWithAdvancedSearch) SearchNotes(
	ctx context.Context,
	query string,
	sort string,
	window *time.Duration,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if r.searchNotesFn == nil {
		return nil, nil
	}
	return r.searchNotesFn(ctx, query, sort, window, limit, offset)
}

func (r readerWithAdvancedSearch) SearchProfilesWithOptions(
	ctx context.Context,
	query string,
	sort string,
	limit int,
	offset int,
) ([]Profile, error) {
	if r.searchProfilesFn == nil {
		return nil, nil
	}
	return r.searchProfilesFn(ctx, query, sort, limit, offset)
}

func (r readerWithAdvancedSearch) GetTrustQualifications(
	ctx context.Context,
	pubkeys []string,
	policy TrustQualificationPolicy,
) (map[string]TrustQualification, error) {
	if r.getTrustQualificationsFn == nil {
		return map[string]TrustQualification{}, nil
	}
	return r.getTrustQualificationsFn(ctx, pubkeys, policy)
}

func (r readerWithAdvancedSearch) IsTrustedAuthor(
	ctx context.Context,
	pubkey string,
	policy TrustQualificationPolicy,
) (bool, error) {
	if r.isTrustedAuthorFn == nil {
		return false, nil
	}
	return r.isTrustedAuthorFn(ctx, pubkey, policy)
}

type readerWithSearchFallback struct {
	Reader
	searchEventsFn   func(context.Context, string, int) ([]json.RawMessage, error)
	searchProfilesFn func(context.Context, string, int) ([]Profile, error)
}

func (r readerWithSearchFallback) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	if r.searchEventsFn == nil {
		return r.Reader.SearchEventsByContent(ctx, query, limit)
	}
	return r.searchEventsFn(ctx, query, limit)
}

func (r readerWithSearchFallback) SearchProfiles(ctx context.Context, query string, limit int) ([]Profile, error) {
	if r.searchProfilesFn == nil {
		return r.Reader.SearchProfiles(ctx, query, limit)
	}
	return r.searchProfilesFn(ctx, query, limit)
}

type readerWithSuggestionSearch struct {
	Reader
	suggestProfilesFn func(context.Context, string, int) ([]Profile, error)
	suggestHashtagsFn func(context.Context, string, int) ([]HashtagSuggestion, error)
}

func (r readerWithSuggestionSearch) SuggestProfiles(ctx context.Context, query string, limit int) ([]Profile, error) {
	if r.suggestProfilesFn == nil {
		return nil, nil
	}
	return r.suggestProfilesFn(ctx, query, limit)
}

func (r readerWithSuggestionSearch) SuggestHashtags(ctx context.Context, query string, limit int) ([]HashtagSuggestion, error) {
	if r.suggestHashtagsFn == nil {
		return nil, nil
	}
	return r.suggestHashtagsFn(ctx, query, limit)
}

func mustRawEvent(t *testing.T, id string, pubkey string) json.RawMessage {
	t.Helper()
	return json.RawMessage(fmt.Sprintf(`{"id":"%s","pubkey":"%s"}`, id, pubkey))
}

func eventID(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var event struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	return event.ID
}
