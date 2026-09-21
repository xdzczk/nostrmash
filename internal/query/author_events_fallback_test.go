package query

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type authorEventsFakeReader struct {
	fakeReader
	getAuthorRecentEventsFn func(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
}

func (f authorEventsFakeReader) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getAuthorRecentEventsFn != nil {
		return f.getAuthorRecentEventsFn(ctx, pubkey, limit)
	}
	return []json.RawMessage{}, nil
}

type fakeAuthorEventsFallbackReader struct {
	fakeFallbackReader
	fetchEventsByAuthorFn func(ctx context.Context, pubkey string, kinds []int, limit int) ([]json.RawMessage, error)
}

func (f fakeAuthorEventsFallbackReader) FetchEventsByAuthor(ctx context.Context, pubkey string, kinds []int, limit int) ([]json.RawMessage, error) {
	if f.fetchEventsByAuthorFn == nil {
		return []json.RawMessage{}, nil
	}
	return f.fetchEventsByAuthorFn(ctx, pubkey, kinds, limit)
}

type capturingEventPersister struct {
	persisted chan string
}

func (p capturingEventPersister) PersistFallbackEvent(_ context.Context, eventID string, _ json.RawMessage) error {
	p.persisted <- eventID
	return nil
}

func authorEventJSON(id, pubkey string, createdAt int64) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"id":%q,"pubkey":%q,"created_at":%d,"kind":1}`, id, pubkey, createdAt))
}

func TestGetAuthorEvents_ThinProfileFallbackMergesAndPersists(t *testing.T) {
	t.Parallel()
	const pubkey = "pk_thin_author"
	persisted := make(chan string, 4)
	svc := mustNewServiceWithOptions(t, authorEventsFakeReader{
		getAuthorRecentEventsFn: func(_ context.Context, _ string, _ int) ([]json.RawMessage, error) {
			return []json.RawMessage{authorEventJSON("evt_local", pubkey, 500)}, nil
		},
	}, ServiceOptions{
		FallbackAuthorEventsMinResults: 5,
		FallbackEventPersister:         capturingEventPersister{persisted: persisted},
		FallbackReader: fakeAuthorEventsFallbackReader{
			fetchEventsByAuthorFn: func(_ context.Context, gotPubkey string, kinds []int, limit int) ([]json.RawMessage, error) {
				if gotPubkey != pubkey {
					t.Errorf("unexpected fallback pubkey: %q", gotPubkey)
				}
				if !reflect.DeepEqual(kinds, []int{1, 30023}) {
					t.Errorf("unexpected fallback kinds: %v", kinds)
				}
				if limit != 20 {
					t.Errorf("unexpected fallback limit: %d", limit)
				}
				return []json.RawMessage{
					authorEventJSON("evt_relay_new", pubkey, 900),
					authorEventJSON("evt_local", pubkey, 500), // duplicate of local
					authorEventJSON("evt_relay_old", pubkey, 100),
					authorEventJSON("evt_wrong_author", "pk_other", 950),
				}, nil
			},
		},
	})

	events, err := svc.GetAuthorEvents(context.Background(), pubkey, 20)
	if err != nil {
		t.Fatalf("GetAuthorEvents: %v", err)
	}
	got := decodeTestEventIDs(t, events)
	want := []string{"evt_relay_new", "evt_local", "evt_relay_old"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected merged events: got=%v want=%v", got, want)
	}

	persistedIDs := map[string]bool{}
	for range 2 {
		select {
		case id := <-persisted:
			persistedIDs[id] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for fallback persistence, got %v", persistedIDs)
		}
	}
	if !persistedIDs["evt_relay_new"] || !persistedIDs["evt_relay_old"] {
		t.Fatalf("expected both relay events persisted, got %v", persistedIDs)
	}
	select {
	case id := <-persisted:
		t.Fatalf("unexpected extra persisted event: %s", id)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestGetAuthorEvents_FallbackSkippedWhenEnoughLocalRows(t *testing.T) {
	t.Parallel()
	const pubkey = "pk_full_author"
	svc := mustNewServiceWithOptions(t, authorEventsFakeReader{
		getAuthorRecentEventsFn: func(_ context.Context, _ string, _ int) ([]json.RawMessage, error) {
			return []json.RawMessage{
				authorEventJSON("evt_1", pubkey, 300),
				authorEventJSON("evt_2", pubkey, 200),
				authorEventJSON("evt_3", pubkey, 100),
			}, nil
		},
	}, ServiceOptions{
		FallbackAuthorEventsMinResults: 3,
		FallbackReader: fakeAuthorEventsFallbackReader{
			fetchEventsByAuthorFn: func(context.Context, string, []int, int) ([]json.RawMessage, error) {
				t.Errorf("fallback must not run when local rows meet the threshold")
				return nil, nil
			},
		},
	})
	events, err := svc.GetAuthorEvents(context.Background(), pubkey, 20)
	if err != nil {
		t.Fatalf("GetAuthorEvents: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("unexpected event count: %d", len(events))
	}
}

func TestGetAuthorEvents_FallbackDisabledByDefault(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, authorEventsFakeReader{
		getAuthorRecentEventsFn: func(context.Context, string, int) ([]json.RawMessage, error) {
			return []json.RawMessage{}, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeAuthorEventsFallbackReader{
			fetchEventsByAuthorFn: func(context.Context, string, []int, int) ([]json.RawMessage, error) {
				t.Errorf("fallback must not run when the thin-profile threshold is unset")
				return nil, nil
			},
		},
	})
	events, err := svc.GetAuthorEvents(context.Background(), "pk_any", 20)
	if err != nil {
		t.Fatalf("GetAuthorEvents: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("unexpected events: %d", len(events))
	}
}

func TestGetAuthorEvents_FallbackDeniedInTrustedOnlyMode(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, authorEventsFakeReader{
		getAuthorRecentEventsFn: func(context.Context, string, int) ([]json.RawMessage, error) {
			return []json.RawMessage{}, nil
		},
	}, ServiceOptions{
		FallbackAuthorEventsMinResults: 5,
		FallbackFetchTrustMode:         "trusted_only",
		FallbackReader: fakeAuthorEventsFallbackReader{
			fetchEventsByAuthorFn: func(context.Context, string, []int, int) ([]json.RawMessage, error) {
				t.Errorf("thin-profile fallback must be denied for untrusted authors in trusted_only mode")
				return nil, nil
			},
		},
	})
	events, err := svc.GetAuthorEvents(context.Background(), "pk_untrusted", 20)
	if err != nil {
		t.Fatalf("GetAuthorEvents: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("unexpected events: %d", len(events))
	}
}

func TestGetAuthorEvents_FallbackErrorDegradesToLocalRows(t *testing.T) {
	t.Parallel()
	const pubkey = "pk_degraded_author"
	svc := mustNewServiceWithOptions(t, authorEventsFakeReader{
		getAuthorRecentEventsFn: func(context.Context, string, int) ([]json.RawMessage, error) {
			return []json.RawMessage{authorEventJSON("evt_local_only", pubkey, 100)}, nil
		},
	}, ServiceOptions{
		FallbackAuthorEventsMinResults: 5,
		FallbackReader: fakeAuthorEventsFallbackReader{
			fetchEventsByAuthorFn: func(context.Context, string, []int, int) ([]json.RawMessage, error) {
				return nil, fmt.Errorf("all fallback relay queries failed")
			},
		},
	})
	events, err := svc.GetAuthorEvents(context.Background(), pubkey, 20)
	if err != nil {
		t.Fatalf("GetAuthorEvents must not fail on fallback errors: %v", err)
	}
	if got := decodeTestEventIDs(t, events); !reflect.DeepEqual(got, []string{"evt_local_only"}) {
		t.Fatalf("unexpected degraded events: %v", got)
	}
}

func TestGetAuthorEventsByKind_PassesKindToFallback(t *testing.T) {
	t.Parallel()
	const pubkey = "pk_kind_author"
	fetched := false
	svc := mustNewServiceWithOptions(t, authorEventsFakeReader{}, ServiceOptions{
		FallbackAuthorEventsMinResults: 5,
		FallbackReader: fakeAuthorEventsFallbackReader{
			fetchEventsByAuthorFn: func(_ context.Context, _ string, kinds []int, _ int) ([]json.RawMessage, error) {
				fetched = true
				if !reflect.DeepEqual(kinds, []int{1}) {
					t.Errorf("unexpected fallback kinds: %v", kinds)
				}
				return []json.RawMessage{}, nil
			},
		},
	})
	if _, err := svc.GetAuthorEventsByKind(context.Background(), pubkey, 1, 20); err != nil {
		t.Fatalf("GetAuthorEventsByKind: %v", err)
	}
	if !fetched {
		t.Fatalf("expected thin-profile fallback to run for kind-filtered reads")
	}
}

func TestMergeAuthorEvents_OrdersDedupesAndTrims(t *testing.T) {
	t.Parallel()
	const pubkey = "pk_merge"
	local := []json.RawMessage{
		authorEventJSON("evt_b", pubkey, 200),
		authorEventJSON("evt_a", pubkey, 100),
	}
	fetched := []json.RawMessage{
		authorEventJSON("evt_c", pubkey, 300),
		authorEventJSON("evt_b", pubkey, 200), // duplicate
		authorEventJSON("evt_other", "pk_other", 400),
		authorEventJSON("evt_tie", pubkey, 200), // created_at tie: id desc
	}
	merged, discovered := mergeAuthorEvents(local, fetched, pubkey, 3)
	got := decodeTestEventIDs(t, merged)
	want := []string{"evt_c", "evt_tie", "evt_b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected merged order: got=%v want=%v", got, want)
	}
	discoveredIDs := make([]string, 0, len(discovered))
	for _, d := range discovered {
		discoveredIDs = append(discoveredIDs, d.id)
	}
	if !reflect.DeepEqual(discoveredIDs, []string{"evt_c", "evt_tie"}) {
		t.Fatalf("unexpected discovered events: %v", discoveredIDs)
	}
}

// pagedAuthorEventsReader adds the keyset page capability on top of the base fake.
type pagedAuthorEventsReader struct {
	fakeReader
	pageFn       func(ctx context.Context, pubkey string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
	pageByKindFn func(ctx context.Context, pubkey string, kind int, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
}

func (f pagedAuthorEventsReader) GetAuthorRecentEventsPage(ctx context.Context, pubkey string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
	if f.pageFn == nil {
		return []json.RawMessage{}, nil, nil
	}
	return f.pageFn(ctx, pubkey, limit, cursor)
}

func (f pagedAuthorEventsReader) GetAuthorRecentEventsByKindPage(ctx context.Context, pubkey string, kind int, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
	if f.pageByKindFn == nil {
		return []json.RawMessage{}, nil, nil
	}
	return f.pageByKindFn(ctx, pubkey, kind, limit, cursor)
}

func TestGetAuthorEventsPage_ReturnsNextCursorAndResumesFromIt(t *testing.T) {
	t.Parallel()
	const pubkey = "pk_paged_author"
	svc := mustNewService(t, pagedAuthorEventsReader{
		pageFn: func(_ context.Context, _ string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
			if limit != 2 {
				t.Errorf("unexpected limit: %d", limit)
			}
			if cursor == nil {
				return []json.RawMessage{
					authorEventJSON("evt_p1", pubkey, 300),
					authorEventJSON("evt_p2", pubkey, 200),
				}, &readmodel.EventOrderCursor{CreatedAt: 200, ID: "evt_p2"}, nil
			}
			if cursor.CreatedAt != 200 || cursor.ID != "evt_p2" {
				t.Errorf("unexpected resume cursor: %+v", cursor)
			}
			return []json.RawMessage{authorEventJSON("evt_p3", pubkey, 100)}, nil, nil
		},
	})

	first, err := svc.GetAuthorEventsPage(context.Background(), pubkey, nil, 2, nil)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if got := decodeTestEventIDs(t, first.Events); !reflect.DeepEqual(got, []string{"evt_p1", "evt_p2"}) {
		t.Fatalf("unexpected first page: %v", got)
	}
	if first.NextCursor == nil || first.NextCursor.CreatedAt != 200 || first.NextCursor.ID != "evt_p2" {
		t.Fatalf("unexpected next cursor: %+v", first.NextCursor)
	}

	second, err := svc.GetAuthorEventsPage(context.Background(), pubkey, nil, 2, first.NextCursor)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if got := decodeTestEventIDs(t, second.Events); !reflect.DeepEqual(got, []string{"evt_p3"}) {
		t.Fatalf("unexpected second page: %v", got)
	}
	if second.NextCursor != nil {
		t.Fatalf("expected no next cursor on final page, got %+v", second.NextCursor)
	}
}

func TestGetAuthorEventsPage_KindFilterUsesKindPagedReader(t *testing.T) {
	t.Parallel()
	const pubkey = "pk_paged_kind_author"
	svc := mustNewService(t, pagedAuthorEventsReader{
		pageByKindFn: func(_ context.Context, _ string, kind int, _ int, _ *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
			if kind != 1 {
				t.Errorf("unexpected kind: %d", kind)
			}
			return []json.RawMessage{authorEventJSON("evt_kind_note", pubkey, 100)}, nil, nil
		},
	})
	kind := 1
	result, err := svc.GetAuthorEventsPage(context.Background(), pubkey, &kind, 20, nil)
	if err != nil {
		t.Fatalf("GetAuthorEventsPage: %v", err)
	}
	if got := decodeTestEventIDs(t, result.Events); !reflect.DeepEqual(got, []string{"evt_kind_note"}) {
		t.Fatalf("unexpected kind page: %v", got)
	}
}

func TestGetAuthorEventsPage_CursorPageSkipsThinProfileFallback(t *testing.T) {
	t.Parallel()
	const pubkey = "pk_paged_cursor_author"
	svc := mustNewServiceWithOptions(t, pagedAuthorEventsReader{
		pageFn: func(context.Context, string, int, *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
			return []json.RawMessage{}, nil, nil
		},
	}, ServiceOptions{
		FallbackAuthorEventsMinResults: 5,
		FallbackReader: fakeAuthorEventsFallbackReader{
			fetchEventsByAuthorFn: func(context.Context, string, []int, int) ([]json.RawMessage, error) {
				t.Errorf("thin-profile fallback must not run on continuation pages")
				return nil, nil
			},
		},
	})
	if _, err := svc.GetAuthorEventsPage(context.Background(), pubkey, nil, 20, &EventCursor{CreatedAt: 100, ID: "evt_x"}); err != nil {
		t.Fatalf("GetAuthorEventsPage: %v", err)
	}
}

func TestGetAuthorEventsPage_LegacyReaderServesFirstPageWithoutCursor(t *testing.T) {
	t.Parallel()
	const pubkey = "pk_legacy_author"
	svc := mustNewService(t, authorEventsFakeReader{
		getAuthorRecentEventsFn: func(context.Context, string, int) ([]json.RawMessage, error) {
			return []json.RawMessage{authorEventJSON("evt_legacy", pubkey, 100)}, nil
		},
	})
	result, err := svc.GetAuthorEventsPage(context.Background(), pubkey, nil, 20, nil)
	if err != nil {
		t.Fatalf("GetAuthorEventsPage: %v", err)
	}
	if got := decodeTestEventIDs(t, result.Events); !reflect.DeepEqual(got, []string{"evt_legacy"}) {
		t.Fatalf("unexpected legacy page: %v", got)
	}
	if result.NextCursor != nil {
		t.Fatalf("legacy readers must not issue cursors, got %+v", result.NextCursor)
	}
}

func decodeTestEventIDs(t *testing.T, rows []json.RawMessage) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(row, &payload); err != nil {
			t.Fatalf("decode event row: %v", err)
		}
		out = append(out, payload.ID)
	}
	return out
}
