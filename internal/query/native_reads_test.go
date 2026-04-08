package query

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetEventWithProvenance_LocalHitIsStrong(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	})
	svc.reader = readerWithProvenance{
		Reader: svc.reader,
		getEventWithProvenanceFn: func(context.Context, string) (EventWithProvenance, error) {
			return EventWithProvenance{
				Event: json.RawMessage(`{"id":"evt-1"}`),
				Relays: []model.EventRelay{
					{EventID: "evt-1", RelayURL: "wss://relay.example", SeenAt: time.Date(2026, 4, 7, 12, 0, 0, 0, time.FixedZone("X", 3600))},
				},
			}, nil
		},
	}
	out, err := svc.GetEventWithProvenance(context.Background(), "evt-1")
	if err != nil {
		t.Fatalf("GetEventWithProvenance returned error: %v", err)
	}
	if out.Consistency != "strong" {
		t.Fatalf("expected strong consistency, got %q", out.Consistency)
	}
	if len(out.Relays) != 1 || out.Relays[0].SeenAt.Location() != time.UTC {
		t.Fatalf("expected UTC relay timestamps, got %#v", out.Relays)
	}
}

func TestGetEventWithProvenance_LocalMissUsesFallback(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, readerWithProvenance{
		Reader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getEventWithProvenanceFn: func(context.Context, string) (EventWithProvenance, error) {
			return EventWithProvenance{}, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				return map[string]json.RawMessage{"evt-1": json.RawMessage(`{"id":"evt-1"}`)}, nil
			},
		},
	})
	out, err := svc.GetEventWithProvenance(context.Background(), "evt-1")
	if err != nil {
		t.Fatalf("GetEventWithProvenance returned error: %v", err)
	}
	if out.Consistency != "eventual" {
		t.Fatalf("expected eventual consistency, got %q", out.Consistency)
	}
	if len(out.Relays) != 0 {
		t.Fatalf("expected no relays for fallback result, got %#v", out.Relays)
	}
}

func TestGetEventReplies_NormalizesLimit(t *testing.T) {
	t.Parallel()
	calledLimit := 0
	svc := mustNewService(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	})
	svc.reader = readerWithReplies{
		Reader: svc.reader,
		getEventRepliesFn: func(_ context.Context, _ string, limit int, _ *EventCursor) ([]json.RawMessage, *EventCursor, error) {
			calledLimit = limit
			return []json.RawMessage{json.RawMessage(`{"id":"reply-1"}`)}, nil, nil
		},
	}
	_, err := svc.GetEventReplies(context.Background(), "evt-1", 0, nil)
	if err != nil {
		t.Fatalf("GetEventReplies returned error: %v", err)
	}
	if calledLimit != 20 {
		t.Fatalf("expected default limit 20, got %d", calledLimit)
	}
}

func TestGetBookmarks_UsesReplaceableBeforeKindFallback(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, bookmarksReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getParameterizedReplaceableEventFn: func(context.Context, string, int, string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"bookmark-replaceable"}`), nil
		},
		getRecentEventsByKindAndPubkeyFn: func(context.Context, int, string, int) ([]json.RawMessage, error) {
			t.Fatal("kind fallback should not run when replaceable exists")
			return nil, nil
		},
	})
	out, err := svc.GetBookmarks(context.Background(), "pk-1", 20)
	if err != nil {
		t.Fatalf("GetBookmarks returned error: %v", err)
	}
	if len(out) != 1 || string(out[0]) != `{"id":"bookmark-replaceable"}` {
		t.Fatalf("unexpected bookmarks payload: %#v", out)
	}
}

type readerWithProvenance struct {
	Reader
	getEventWithProvenanceFn func(context.Context, string) (EventWithProvenance, error)
}

func (r readerWithProvenance) GetEventWithProvenance(ctx context.Context, id string) (EventWithProvenance, error) {
	if r.getEventWithProvenanceFn == nil {
		return EventWithProvenance{}, store.ErrNotFound
	}
	return r.getEventWithProvenanceFn(ctx, id)
}

type readerWithReplies struct {
	Reader
	getEventRepliesFn func(context.Context, string, int, *EventCursor) ([]json.RawMessage, *EventCursor, error)
}

func (r readerWithReplies) GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error) {
	if r.getEventRepliesFn == nil {
		return []json.RawMessage{}, nil, nil
	}
	return r.getEventRepliesFn(ctx, eventID, limit, cursor)
}

type bookmarksReader struct {
	fakeReader
	getParameterizedReplaceableEventFn func(context.Context, string, int, string) (json.RawMessage, error)
	getRecentEventsByKindAndPubkeyFn   func(context.Context, int, string, int) ([]json.RawMessage, error)
}

func (r bookmarksReader) GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
	if r.getParameterizedReplaceableEventFn == nil {
		return nil, store.ErrNotFound
	}
	return r.getParameterizedReplaceableEventFn(ctx, pubkey, kind, dTag)
}

func (r bookmarksReader) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	if r.getRecentEventsByKindAndPubkeyFn == nil {
		return r.fakeReader.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
	}
	return r.getRecentEventsByKindAndPubkeyFn(ctx, kind, pubkey, limit)
}

func (r bookmarksReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	return r.fakeReader.GetEventRawByID(ctx, id)
}

func (r bookmarksReader) GetEventWithProvenance(ctx context.Context, id string) (EventWithProvenance, error) {
	return r.fakeReader.GetEventWithProvenance(ctx, id)
}

func (r bookmarksReader) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return r.fakeReader.GetEventRawsByIDs(ctx, ids)
}

func (r bookmarksReader) GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error) {
	return r.fakeReader.GetEventSeenOn(ctx, id)
}

func (r bookmarksReader) GetProfileByPubkey(ctx context.Context, pubkey string) (Profile, error) {
	return r.fakeReader.GetProfileByPubkey(ctx, pubkey)
}

func (r bookmarksReader) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	return r.fakeReader.GetProfilesByPubkeys(ctx, pubkeys)
}

func (r bookmarksReader) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return r.fakeReader.GetAuthorRecentEvents(ctx, pubkey, limit)
}

func (r bookmarksReader) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return r.fakeReader.GetAuthorReplies(ctx, pubkey, limit)
}

func (r bookmarksReader) GetEventCounts(ctx context.Context, eventID string) (EventCounts, error) {
	return r.fakeReader.GetEventCounts(ctx, eventID)
}

func (r bookmarksReader) GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error) {
	return r.fakeReader.GetEventReplies(ctx, eventID, limit, cursor)
}

func (r bookmarksReader) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
	return r.fakeReader.GetEventAncestors(ctx, eventID, maxDepth)
}

func (r bookmarksReader) ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	return r.fakeReader.ListRelayHealth(ctx)
}

func (r bookmarksReader) GetContactListByPubkey(ctx context.Context, pubkey string) (ContactList, error) {
	return r.fakeReader.GetContactListByPubkey(ctx, pubkey)
}

func (r bookmarksReader) GetRelayListByPubkey(ctx context.Context, pubkey string) (RelayList, error) {
	return r.fakeReader.GetRelayListByPubkey(ctx, pubkey)
}

func (r bookmarksReader) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	return r.fakeReader.SearchEventsByContent(ctx, query, limit)
}

func (r bookmarksReader) SearchProfiles(ctx context.Context, query string, limit int) ([]Profile, error) {
	return r.fakeReader.SearchProfiles(ctx, query, limit)
}

func (r bookmarksReader) GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	return r.fakeReader.GetEventsReferencingPubkey(ctx, targetPubkey, limit)
}

func (r bookmarksReader) GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	return r.fakeReader.GetFollowersByPubkey(ctx, targetPubkey, limit)
}

func TestGetEventWithProvenance_LocalMissFallbackMissReturnsNotFound(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, readerWithProvenance{
		Reader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getEventWithProvenanceFn: func(context.Context, string) (EventWithProvenance, error) {
			return EventWithProvenance{}, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				return map[string]json.RawMessage{}, nil
			},
		},
	})
	_, err := svc.GetEventWithProvenance(context.Background(), "evt-missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected store.ErrNotFound, got %v", err)
	}
}
