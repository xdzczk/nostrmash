package api

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func mustNewHandlers(tb testing.TB, reader EventReader, maxBatchSize int) Handlers {
	tb.Helper()
	handlers, err := NewHandlers(reader, maxBatchSize)
	if err != nil {
		tb.Fatalf("NewHandlers: %v", err)
	}
	return handlers
}

func mustNewHandlersWithOptions(tb testing.TB, reader EventReader, options HandlersOptions) Handlers {
	tb.Helper()
	handlers, err := NewHandlersWithOptions(reader, options)
	if err != nil {
		tb.Fatalf("NewHandlersWithOptions: %v", err)
	}
	return handlers
}

type fakeEventReader struct {
	getEventRawByIDFn             func(context.Context, string) (json.RawMessage, error)
	getEventWithProvFn            func(context.Context, string) (store.EventWithProvenance, error)
	getEventRawsByIDs             func(context.Context, []string) (map[string]json.RawMessage, error)
	getEventSeenOnByID            func(context.Context, string) ([]model.EventRelay, error)
	getProfileByPubkey            func(context.Context, string) (store.ProfileProjection, error)
	getProfilesByBatch            func(context.Context, []string) (map[string]store.ProfileProjection, error)
	getProfilePublicStatsByPubkey func(context.Context, string) (store.ProfilePublicStatsProjection, error)
	getAuthorEventsFn             func(context.Context, string, int) ([]json.RawMessage, error)
	getAuthorRepliesFn            func(context.Context, string, int) ([]json.RawMessage, error)
	getEventCountsFn              func(context.Context, string) (store.EventCounts, error)
	getEventRepliesFn             func(context.Context, string, int, *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	getEventAncestors             func(context.Context, string, int) ([]json.RawMessage, []string, error)
	listRelayHealthFn             func(context.Context) ([]model.IngestCheckpoint, error)
	getContactListFn              func(context.Context, string) (store.ContactListProjection, error)
	getRelayListFn                func(context.Context, string) (store.RelayListProjection, error)
	searchEventsFn                func(context.Context, string, int) ([]json.RawMessage, error)
	searchProfilesFn              func(context.Context, string, int) ([]store.ProfileProjection, error)
	searchNotesFn                 func(context.Context, string, string, *time.Duration, int, int) ([]json.RawMessage, error)
	searchProfilesWithOptionsFn   func(context.Context, string, string, int, int) ([]store.ProfileProjection, error)
	suggestProfilesFn             func(context.Context, string, int) ([]store.ProfileProjection, error)
	suggestHashtagsFn             func(context.Context, string, int) ([]store.TrendingHashtag, error)
	getByKindPubkeyFn             func(context.Context, int, string, int) ([]json.RawMessage, error)
	getRefsPubkeyFn               func(context.Context, string, int) ([]json.RawMessage, error)
	getFollowersFn                func(context.Context, string, int) ([]json.RawMessage, error)
	getTrustScoreFn               func(context.Context, string) (store.TrustGlobalScore, error)
	listTopTrustFn                func(context.Context, int) ([]store.TrustGlobalScore, error)
	getTrustRunFn                 func(context.Context, int64) (store.TrustRun, error)
	listTrustRunsFn               func(context.Context, int) ([]store.TrustRun, error)
	getNetworkStatsFn             func(context.Context) (store.NetworkStats, error)
	getPublicNetworkStatsFn       func(context.Context, int) (store.PublicDiscoveryNetworkStats, error)
	getCuratedReadsFn             func(context.Context, int) ([]store.CuratedRecommendedRead, error)
	getCuratedTopicsFn            func(context.Context, int) ([]store.CuratedReadsTopic, error)
	getTrendingNotesFn            func(context.Context, time.Duration, int, int) ([]store.TrendingNote, error)
	getTrendingTagsFn             func(context.Context, time.Duration, int, int) ([]store.TrendingHashtag, error)
	getTrendingProfilesFn         func(context.Context, time.Duration, int, int) ([]store.TrendingProfile, error)
	getRisingProfilesFn           func(context.Context, time.Duration, int, int) ([]store.TrendingProfile, error)
	getCuratedPubsFn              func(context.Context, int) ([]store.CuratedFeaturedAuthor, error)
}

type trustQualifiedFakeReader struct {
	fakeEventReader
	getTrustQualificationsFn func(context.Context, []string, store.TrustQualificationPolicy) (map[string]store.TrustQualification, error)
	isTrustedAuthorFn        func(context.Context, string, store.TrustQualificationPolicy) (bool, error)
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

func (f fakeEventReader) GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (store.ProfilePublicStatsProjection, error) {
	if f.getProfilePublicStatsByPubkey == nil {
		return store.ProfilePublicStatsProjection{Pubkey: pubkey}, nil
	}
	return f.getProfilePublicStatsByPubkey(ctx, pubkey)
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

func (f fakeEventReader) SearchNotes(
	ctx context.Context,
	query string,
	sort string,
	window *time.Duration,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if f.searchNotesFn != nil {
		return f.searchNotesFn(ctx, query, sort, window, limit, offset)
	}
	if f.searchEventsFn != nil && sort == "relevant" && window == nil && offset == 0 {
		return f.searchEventsFn(ctx, query, limit)
	}
	return nil, errors.New("not implemented")
}

func (f fakeEventReader) SearchProfilesWithOptions(
	ctx context.Context,
	query string,
	sort string,
	limit int,
	offset int,
) ([]store.ProfileProjection, error) {
	if f.searchProfilesWithOptionsFn != nil {
		return f.searchProfilesWithOptionsFn(ctx, query, sort, limit, offset)
	}
	if f.searchProfilesFn != nil && sort == "relevant" && offset == 0 {
		return f.searchProfilesFn(ctx, query, limit)
	}
	return nil, errors.New("not implemented")
}

func (f fakeEventReader) SuggestProfiles(ctx context.Context, query string, limit int) ([]store.ProfileProjection, error) {
	if f.suggestProfilesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.suggestProfilesFn(ctx, query, limit)
}

func (f fakeEventReader) SuggestHashtags(ctx context.Context, query string, limit int) ([]store.TrendingHashtag, error) {
	if f.suggestHashtagsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.suggestHashtagsFn(ctx, query, limit)
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

func (f fakeEventReader) GetNetworkStats(ctx context.Context) (store.NetworkStats, error) {
	if f.getNetworkStatsFn == nil {
		return store.NetworkStats{}, errors.New("not implemented")
	}
	return f.getNetworkStatsFn(ctx)
}

func (f fakeEventReader) GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (store.PublicDiscoveryNetworkStats, error) {
	if f.getPublicNetworkStatsFn == nil {
		return store.PublicDiscoveryNetworkStats{}, errors.New("not implemented")
	}
	return f.getPublicNetworkStatsFn(ctx, hashtagLimit)
}

func (f fakeEventReader) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error) {
	if f.getCuratedReadsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getCuratedReadsFn(ctx, limit)
}

func (f fakeEventReader) GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error) {
	if f.getCuratedTopicsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getCuratedTopicsFn(ctx, limit)
}

func (f fakeEventReader) GetTrendingHashtags(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]store.TrendingHashtag, error) {
	if f.getTrendingTagsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getTrendingTagsFn(ctx, window, limit, offset)
}

func (f fakeEventReader) GetTrendingNotes(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]store.TrendingNote, error) {
	if f.getTrendingNotesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getTrendingNotesFn(ctx, window, limit, offset)
}

func (f fakeEventReader) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error) {
	if f.getCuratedPubsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getCuratedPubsFn(ctx, limit)
}

func (f fakeEventReader) GetTrendingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]store.TrendingProfile, error) {
	if f.getTrendingProfilesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getTrendingProfilesFn(ctx, window, limit, offset)
}

func (f fakeEventReader) GetRisingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]store.TrendingProfile, error) {
	if f.getRisingProfilesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getRisingProfilesFn(ctx, window, limit, offset)
}

func (f trustQualifiedFakeReader) GetTrustQualifications(
	ctx context.Context,
	pubkeys []string,
	policy store.TrustQualificationPolicy,
) (map[string]store.TrustQualification, error) {
	if f.getTrustQualificationsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getTrustQualificationsFn(ctx, pubkeys, policy)
}

func (f trustQualifiedFakeReader) IsTrustedAuthor(
	ctx context.Context,
	pubkey string,
	policy store.TrustQualificationPolicy,
) (bool, error) {
	if f.isTrustedAuthorFn == nil {
		return false, errors.New("not implemented")
	}
	return f.isTrustedAuthorFn(ctx, pubkey, policy)
}
