package query

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetEventByID_LocalMissRelaySuccess(t *testing.T) {
	t.Parallel()
	svc := NewServiceWithOptions(fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
				if len(ids) != 1 || ids[0] != "evt-1" {
					t.Fatalf("unexpected ids for relay fallback: %#v", ids)
				}
				return map[string]json.RawMessage{"evt-1": json.RawMessage(`{"id":"evt-1"}`)}, nil
			},
		},
	})

	raw, err := svc.GetEventByID(context.Background(), "evt-1")
	if err != nil {
		t.Fatalf("GetEventByID returned error: %v", err)
	}
	if string(raw) != `{"id":"evt-1"}` {
		t.Fatalf("unexpected raw event: %s", string(raw))
	}
}

func TestGetEventByID_LocalHitSkipsRelayFallback(t *testing.T) {
	t.Parallel()
	svc := NewServiceWithOptions(fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"evt-local"}`), nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				t.Fatalf("relay fallback should not run on local hit")
				return nil, nil
			},
		},
	})
	raw, err := svc.GetEventByID(context.Background(), "evt-local")
	if err != nil {
		t.Fatalf("GetEventByID returned error: %v", err)
	}
	if string(raw) != `{"id":"evt-local"}` {
		t.Fatalf("unexpected local event payload: %s", string(raw))
	}
}

func TestGetEventByID_LocalMissRelayMiss(t *testing.T) {
	t.Parallel()
	svc := NewServiceWithOptions(fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				return map[string]json.RawMessage{}, nil
			},
		},
	})

	_, err := svc.GetEventByID(context.Background(), "evt-missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected store.ErrNotFound, got %v", err)
	}
}

func TestGetEventBatch_LocalMissRelaySuccess(t *testing.T) {
	t.Parallel()
	svc := NewServiceWithOptions(fakeReader{
		getEventRawsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"evt-1": json.RawMessage(`{"id":"evt-1"}`),
			}, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
				if len(ids) != 1 || ids[0] != "evt-2" {
					t.Fatalf("unexpected fallback ids: %#v", ids)
				}
				return map[string]json.RawMessage{
					"evt-2": json.RawMessage(`{"id":"evt-2"}`),
				}, nil
			},
		},
	})

	out, err := svc.GetEventBatch(context.Background(), []string{"evt-1", "evt-2"})
	if err != nil {
		t.Fatalf("GetEventBatch returned error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("unexpected batch size: %d", len(out))
	}
}

func TestGetEventBatch_LocalMissRelayPartialSuccess(t *testing.T) {
	t.Parallel()
	svc := NewServiceWithOptions(fakeReader{
		getEventRawsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"evt-1": json.RawMessage(`{"id":"evt-1"}`),
			}, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
				if len(ids) != 2 {
					t.Fatalf("expected 2 missing ids for fallback, got %#v", ids)
				}
				return map[string]json.RawMessage{
					"evt-2": json.RawMessage(`{"id":"evt-2"}`),
				}, nil
			},
		},
	})

	out, err := svc.GetEventBatch(context.Background(), []string{"evt-1", "evt-2", "evt-3"})
	if err != nil {
		t.Fatalf("GetEventBatch returned error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected partial batch size 2, got %d", len(out))
	}
	if _, ok := out["evt-3"]; ok {
		t.Fatalf("did not expect unresolved fallback id evt-3 in output")
	}
}

func TestGetProfile_LocalMissRelaySuccess(t *testing.T) {
	t.Parallel()
	svc := NewServiceWithOptions(fakeReader{
		getProfileByPubkeyFn: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{}, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchProfilesByPubkeysFn: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
				if len(pubkeys) != 1 || pubkeys[0] != "pk-1" {
					t.Fatalf("unexpected pubkeys for fallback: %#v", pubkeys)
				}
				return map[string]store.ProfileProjection{
					"pk-1": {
						Pubkey:            "pk-1",
						MetadataEventID:   "meta-1",
						MetadataCreatedAt: 100,
						ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
					},
				}, nil
			},
		},
	})

	profile, err := svc.GetProfile(context.Background(), "pk-1")
	if err != nil {
		t.Fatalf("GetProfile returned error: %v", err)
	}
	if profile.Pubkey != "pk-1" {
		t.Fatalf("unexpected profile pubkey: %s", profile.Pubkey)
	}
}

func TestGetProfiles_LocalMissRelayMissPreservesMissing(t *testing.T) {
	t.Parallel()
	svc := NewServiceWithOptions(fakeReader{
		getProfilesByPubkeysFn: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{}, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchProfilesByPubkeysFn: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
				return map[string]store.ProfileProjection{}, nil
			},
		},
	})

	out, err := svc.GetProfiles(context.Background(), []string{"pk-1", "pk-2"})
	if err != nil {
		t.Fatalf("GetProfiles returned error: %v", err)
	}
	if len(out.Profiles) != 0 {
		t.Fatalf("expected no profiles, got %#v", out.Profiles)
	}
	if len(out.MissingPubkeys) != 2 {
		t.Fatalf("expected 2 missing pubkeys, got %#v", out.MissingPubkeys)
	}
}

func TestGetProfiles_LocalMissRelayPartialSuccessPreservesMissing(t *testing.T) {
	t.Parallel()
	svc := NewServiceWithOptions(fakeReader{
		getProfilesByPubkeysFn: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{}, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchProfilesByPubkeysFn: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
				return map[string]store.ProfileProjection{
					"pk-1": {
						Pubkey:            "pk-1",
						MetadataEventID:   "meta-1",
						MetadataCreatedAt: 123,
						ProfileJSON:       json.RawMessage(`{"name":"one"}`),
					},
				}, nil
			},
		},
	})

	out, err := svc.GetProfiles(context.Background(), []string{"pk-1", "pk-2"})
	if err != nil {
		t.Fatalf("GetProfiles returned error: %v", err)
	}
	if len(out.Profiles) != 1 {
		t.Fatalf("expected 1 recovered profile, got %d", len(out.Profiles))
	}
	if len(out.MissingPubkeys) != 1 || out.MissingPubkeys[0] != "pk-2" {
		t.Fatalf("expected pk-2 to stay missing, got %#v", out.MissingPubkeys)
	}
}

type fakeReader struct {
	getEventRawByIDFn      func(context.Context, string) (json.RawMessage, error)
	getEventRawsByIDsFn    func(context.Context, []string) (map[string]json.RawMessage, error)
	getProfileByPubkeyFn   func(context.Context, string) (store.ProfileProjection, error)
	getProfilesByPubkeysFn func(context.Context, []string) (map[string]store.ProfileProjection, error)
}

func (f fakeReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	if f.getEventRawByIDFn != nil {
		return f.getEventRawByIDFn(ctx, id)
	}
	return nil, store.ErrNotFound
}

func (f fakeReader) GetEventWithProvenance(context.Context, string) (store.EventWithProvenance, error) {
	return store.EventWithProvenance{}, store.ErrNotFound
}

func (f fakeReader) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.getEventRawsByIDsFn != nil {
		return f.getEventRawsByIDsFn(ctx, ids)
	}
	return map[string]json.RawMessage{}, nil
}

func (f fakeReader) GetEventSeenOn(context.Context, string) ([]model.EventRelay, error) {
	return []model.EventRelay{}, nil
}

func (f fakeReader) GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error) {
	if f.getProfileByPubkeyFn != nil {
		return f.getProfileByPubkeyFn(ctx, pubkey)
	}
	return store.ProfileProjection{}, store.ErrNotFound
}

func (f fakeReader) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
	if f.getProfilesByPubkeysFn != nil {
		return f.getProfilesByPubkeysFn(ctx, pubkeys)
	}
	return map[string]store.ProfileProjection{}, nil
}

func (f fakeReader) GetAuthorRecentEvents(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) GetAuthorReplies(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) GetEventCounts(context.Context, string) (store.EventCounts, error) {
	return store.EventCounts{}, nil
}

func (f fakeReader) GetEventReplies(context.Context, string, int, *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
	return []json.RawMessage{}, nil, nil
}

func (f fakeReader) GetEventAncestors(context.Context, string, int) ([]json.RawMessage, []string, error) {
	return []json.RawMessage{}, []string{}, nil
}

func (f fakeReader) ListRelayHealth(context.Context) ([]model.IngestCheckpoint, error) {
	return []model.IngestCheckpoint{}, nil
}

func (f fakeReader) GetContactListByPubkey(context.Context, string) (store.ContactListProjection, error) {
	return store.ContactListProjection{}, store.ErrNotFound
}

func (f fakeReader) GetRelayListByPubkey(context.Context, string) (store.RelayListProjection, error) {
	return store.RelayListProjection{}, store.ErrNotFound
}

func (f fakeReader) SearchEventsByContent(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) SearchProfiles(context.Context, string, int) ([]store.ProfileProjection, error) {
	return []store.ProfileProjection{}, nil
}

func (f fakeReader) GetRecentEventsByKindAndPubkey(context.Context, int, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) GetEventsReferencingPubkey(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) GetFollowersByPubkey(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}
