package api

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

type fakeEventReader struct {
	getEventRawByIDFn  func(context.Context, string) (json.RawMessage, error)
	getEventWithProvFn func(context.Context, string) (store.EventWithProvenance, error)
	getEventRawsByIDs  func(context.Context, []string) (map[string]json.RawMessage, error)
	getEventSeenOnByID func(context.Context, string) ([]model.EventRelay, error)
	getProfileByPubkey func(context.Context, string) (store.ProfileProjection, error)
	getProfilesByBatch func(context.Context, []string) (map[string]store.ProfileProjection, error)
	getAuthorEventsFn  func(context.Context, string, int) ([]json.RawMessage, error)
	getAuthorRepliesFn func(context.Context, string, int) ([]json.RawMessage, error)
	getEventCountsFn   func(context.Context, string) (store.EventCounts, error)
	getEventRepliesFn  func(context.Context, string, int, *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	getEventAncestors  func(context.Context, string, int) ([]json.RawMessage, []string, error)
	listRelayHealthFn  func(context.Context) ([]model.IngestCheckpoint, error)
	getContactListFn   func(context.Context, string) (store.ContactListProjection, error)
	getRelayListFn     func(context.Context, string) (store.RelayListProjection, error)
	searchEventsFn     func(context.Context, string, int) ([]json.RawMessage, error)
	searchProfilesFn   func(context.Context, string, int) ([]store.ProfileProjection, error)
	getByKindPubkeyFn  func(context.Context, int, string, int) ([]json.RawMessage, error)
	getRefsPubkeyFn    func(context.Context, string, int) ([]json.RawMessage, error)
	getFollowersFn     func(context.Context, string, int) ([]json.RawMessage, error)
	getTrustScoreFn    func(context.Context, string) (store.TrustGlobalScore, error)
	listTopTrustFn     func(context.Context, int) ([]store.TrustGlobalScore, error)
	getTrustRunFn      func(context.Context, int64) (store.TrustRun, error)
	listTrustRunsFn    func(context.Context, int) ([]store.TrustRun, error)
}

func (f fakeEventReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	if f.getEventRawByIDFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventRawByIDFn(ctx, id)
}

func (f fakeEventReader) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.getEventRawsByIDs == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventRawsByIDs(ctx, ids)
}

func (f fakeEventReader) GetEventWithProvenance(ctx context.Context, id string) (store.EventWithProvenance, error) {
	if f.getEventWithProvFn == nil {
		return store.EventWithProvenance{}, errors.New("not implemented")
	}
	return f.getEventWithProvFn(ctx, id)
}

func (f fakeEventReader) GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error) {
	if f.getEventSeenOnByID == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventSeenOnByID(ctx, id)
}

func (f fakeEventReader) GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error) {
	if f.getProfileByPubkey == nil {
		return store.ProfileProjection{}, errors.New("not implemented")
	}
	return f.getProfileByPubkey(ctx, pubkey)
}

func (f fakeEventReader) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
	if f.getProfilesByBatch == nil {
		return nil, errors.New("not implemented")
	}
	return f.getProfilesByBatch(ctx, pubkeys)
}

func (f fakeEventReader) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getAuthorEventsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorEventsFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getAuthorRepliesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorRepliesFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error) {
	if f.getEventCountsFn == nil {
		return store.EventCounts{}, errors.New("not implemented")
	}
	return f.getEventCountsFn(ctx, eventID)
}

func (f fakeEventReader) GetEventReplies(
	ctx context.Context,
	eventID string,
	limit int,
	cursor *store.EventOrderCursor,
) ([]json.RawMessage, *store.EventOrderCursor, error) {
	if f.getEventRepliesFn == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getEventRepliesFn(ctx, eventID, limit, cursor)
}

func (f fakeEventReader) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
	if f.getEventAncestors == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getEventAncestors(ctx, eventID, maxDepth)
}

func (f fakeEventReader) ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	if f.listRelayHealthFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.listRelayHealthFn(ctx)
}

func (f fakeEventReader) GetContactListByPubkey(ctx context.Context, pubkey string) (store.ContactListProjection, error) {
	if f.getContactListFn == nil {
		return store.ContactListProjection{}, errors.New("not implemented")
	}
	return f.getContactListFn(ctx, pubkey)
}

func (f fakeEventReader) GetRelayListByPubkey(ctx context.Context, pubkey string) (store.RelayListProjection, error) {
	if f.getRelayListFn == nil {
		return store.RelayListProjection{}, errors.New("not implemented")
	}
	return f.getRelayListFn(ctx, pubkey)
}

func (f fakeEventReader) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	if f.searchEventsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.searchEventsFn(ctx, query, limit)
}

func (f fakeEventReader) SearchProfiles(ctx context.Context, query string, limit int) ([]store.ProfileProjection, error) {
	if f.searchProfilesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.searchProfilesFn(ctx, query, limit)
}

func (f fakeEventReader) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getByKindPubkeyFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getByKindPubkeyFn(ctx, kind, pubkey, limit)
}

func (f fakeEventReader) GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	if f.getRefsPubkeyFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getRefsPubkeyFn(ctx, targetPubkey, limit)
}

func (f fakeEventReader) GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	if f.getFollowersFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getFollowersFn(ctx, targetPubkey, limit)
}

func (f fakeEventReader) GetTrustScore(ctx context.Context, pubkey string) (store.TrustGlobalScore, error) {
	if f.getTrustScoreFn == nil {
		return store.TrustGlobalScore{}, errors.New("not implemented")
	}
	return f.getTrustScoreFn(ctx, pubkey)
}

func (f fakeEventReader) ListTopTrustedPubkeys(ctx context.Context, limit int) ([]store.TrustGlobalScore, error) {
	if f.listTopTrustFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.listTopTrustFn(ctx, limit)
}

func (f fakeEventReader) GetTrustRun(ctx context.Context, runID int64) (store.TrustRun, error) {
	if f.getTrustRunFn == nil {
		return store.TrustRun{}, errors.New("not implemented")
	}
	return f.getTrustRunFn(ctx, runID)
}

func (f fakeEventReader) ListTrustRuns(ctx context.Context, limit int) ([]store.TrustRun, error) {
	if f.listTrustRunsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.listTrustRunsFn(ctx, limit)
}
