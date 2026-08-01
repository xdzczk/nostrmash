package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	storeread "github.com/xdzczk/nostrmash/internal/store/read"
	storetrust "github.com/xdzczk/nostrmash/internal/store/trust"
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
	getEventRawByIDFn                func(context.Context, string) (json.RawMessage, error)
	getEventWithProvFn               func(context.Context, string) (store.EventWithProvenance, error)
	getEventRawsByIDs                func(context.Context, []string) (map[string]json.RawMessage, error)
	getEventSeenOnByID               func(context.Context, string) ([]model.EventRelay, error)
	getProfileByPubkey               func(context.Context, string) (store.ProfileProjection, error)
	getProfilesByBatch               func(context.Context, []string) (map[string]store.ProfileProjection, error)
	getProfilePublicStatsByPubkey    func(context.Context, string) (store.ProfilePublicStatsProjection, error)
	getAuthorAnalyticsSummaryFn      func(context.Context, string) ([]store.AuthorAnalyticsSummaryProjection, error)
	getAuthorRelayFootprintFn        func(context.Context, string, int) (store.AuthorRelayFootprintProjection, error)
	getAuthorTopicStatsFn            func(context.Context, string, int, int) ([]store.AuthorTopicStatsProjection, error)
	getAuthorMediaMixStatsFn         func(context.Context, string, int) (store.AuthorMediaMixStatsProjection, error)
	getAuthorActivityWindowBucketsFn func(context.Context, string, int) ([]store.AuthorActivityWindowBucketProjection, error)
	getAuthorPostingPatternBucketsFn func(context.Context, string, int) ([]store.AuthorPostingPatternBucketProjection, error)
	getAuthorTopNotesFn              func(context.Context, string, int, int) ([]store.AuthorTopNoteProjection, error)
	getGroupedNoteAnalyticsFn        func(context.Context, store.GroupedNoteAnalyticsQuery) (store.GroupedNoteAnalyticsProjection, error)
	getAuthorRecycleCandidatesFn     func(context.Context, string, int, int, float64, bool, bool, int, int) ([]store.AuthorRecycleCandidateProjection, error)
	getAuthorPerformanceAggregateFn  func(context.Context, string, int) (store.AuthorPerformanceAggregateProjection, store.AuthorPerformanceAggregateProjection, error)
	getAuthorEventsFn                func(context.Context, string, int) ([]json.RawMessage, error)
	getAuthorRecentEventsByKindFn    func(context.Context, string, int, int) ([]json.RawMessage, error)
	getAuthorRepliesFn               func(context.Context, string, int) ([]json.RawMessage, error)
	getAuthorSentZapsFn              func(context.Context, string, int, *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	getAuthorReactionsFn             func(context.Context, string, int, *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	getEventCountsFn                 func(context.Context, string) (store.EventCounts, error)
	getEventRepliesFn                func(context.Context, string, int, *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	getEventAncestors                func(context.Context, string, int) ([]json.RawMessage, []string, error)
	getThreadSummaryFn               func(context.Context, string) (store.ThreadSummaryProjection, error)
	listRelayHealthFn                func(context.Context) ([]model.IngestCheckpoint, error)
	getContactListFn                 func(context.Context, string) (storeread.ContactListProjection, error)
	getRelayListFn                   func(context.Context, string) (storeread.RelayListProjection, error)
	searchEventsFn                   func(context.Context, string, int) ([]json.RawMessage, error)
	searchProfilesFn                 func(context.Context, string, int) ([]store.ProfileProjection, error)
	searchNotesFn                    func(context.Context, string, string, *time.Duration, string, int, int) ([]json.RawMessage, error)
	searchProfilesWithOptionsFn      func(context.Context, string, string, int, int) ([]store.ProfileProjection, error)
	suggestProfilesFn                func(context.Context, string, int) ([]store.ProfileProjection, error)
	suggestHashtagsFn                func(context.Context, string, int) ([]storeread.TrendingHashtag, error)
	getByKindPubkeyFn                func(context.Context, int, string, int) ([]json.RawMessage, error)
	getRefsPubkeyFn                  func(context.Context, string, int) ([]json.RawMessage, error)
	getFollowersFn                   func(context.Context, string, int) ([]json.RawMessage, error)
	getTrustScoreFn                  func(context.Context, string) (storetrust.TrustGlobalScore, error)
	listTopTrustFn                   func(context.Context, int) ([]storetrust.TrustGlobalScore, error)
	getTrustRunFn                    func(context.Context, int64) (storetrust.TrustRun, error)
	listTrustRunsFn                  func(context.Context, int) ([]storetrust.TrustRun, error)
	getNetworkStatsFn                func(context.Context) (storeread.NetworkStats, error)
	getPublicNetworkStatsFn          func(context.Context, int) (storeread.PublicDiscoveryNetworkStats, error)
	getDiscoveryStatsSeriesFn        func(context.Context, string, time.Duration) (storeread.DiscoveryStatsSeries, error)
	getCuratedReadsFn                func(context.Context, int) ([]storeread.CuratedRecommendedRead, error)
	getCuratedTopicsFn               func(context.Context, int) ([]storeread.CuratedReadsTopic, error)
	getTrendingNotesFn               func(context.Context, time.Duration, int, int) ([]storeread.TrendingNote, error)
	getHotConversationsFn            func(context.Context, time.Duration, int, int) ([]store.HotConversation, error)
	getTrendingTagsFn                func(context.Context, time.Duration, int, int) ([]storeread.TrendingHashtag, error)
	getHashtagSummaryFn              func(context.Context, string) (storeread.HashtagSummary, error)
	getHashtagNotesFn                func(context.Context, string, string, string, int, int) ([]storeread.TrendingNote, error)
	getRelatedHashtagsFn             func(context.Context, string, int) ([]storeread.RelatedHashtag, error)
	getTrendingDomainsFn             func(context.Context, time.Duration, int, int) ([]store.DomainSummaryProjection, error)
	getHomeTrendingDomainsFn         func(context.Context, time.Duration, int) ([]store.DomainSummaryProjection, error)
	getDomainSummaryFn               func(context.Context, string, int, int) (store.DomainSummaryProjection, error)
	getDomainNotesFn                 func(context.Context, string, string, string, int, int) ([]storeread.TrendingNote, error)
	getTrendingProfilesFn            func(context.Context, time.Duration, int, int) ([]storeread.TrendingProfile, error)
	getRisingProfilesFn              func(context.Context, time.Duration, int, int) ([]storeread.TrendingProfile, error)
	getRelatedProfilesFn             func(context.Context, string, int) ([]storeread.RelatedProfile, error)
	getCuratedPubsFn                 func(context.Context, int) ([]storeread.CuratedFeaturedAuthor, error)
	getNoteStatsFn                   func(context.Context, string) (storeread.NoteStats, error)
	getNoteConversationVelocityFn    func(context.Context, string) (storeread.NoteConversationVelocity, error)
	getNoteQuoteRepostLinkageFn      func(context.Context, string, int) (store.NoteQuoteRepostLinkageProjection, error)
	getRelatedNotesFn                func(context.Context, string, int) ([]storeread.RelatedNote, error)
}

type trustQualifiedFakeReader struct {
	fakeEventReader
	getTrustQualificationsFn func(context.Context, []string, storetrust.TrustQualificationPolicy) (map[string]storetrust.TrustQualification, error)
	isTrustedAuthorFn        func(context.Context, string, storetrust.TrustQualificationPolicy) (bool, error)
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
		return map[string]store.ProfileProjection{}, nil
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

func (f fakeEventReader) GetAuthorRecentEventsByKind(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error) {
	if f.getAuthorRecentEventsByKindFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorRecentEventsByKindFn(ctx, pubkey, kind, limit)
}

func (f fakeEventReader) GetAuthorSentZaps(
	ctx context.Context,
	pubkey string,
	limit int,
	cursor *store.EventOrderCursor,
) ([]json.RawMessage, *store.EventOrderCursor, error) {
	if f.getAuthorSentZapsFn == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getAuthorSentZapsFn(ctx, pubkey, limit, cursor)
}

func (f fakeEventReader) GetAuthorReactions(
	ctx context.Context,
	pubkey string,
	limit int,
	cursor *store.EventOrderCursor,
) ([]json.RawMessage, *store.EventOrderCursor, error) {
	if f.getAuthorReactionsFn == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getAuthorReactionsFn(ctx, pubkey, limit, cursor)
}

func (f fakeEventReader) GetAuthorAnalyticsSummary(
	ctx context.Context,
	pubkey string,
) ([]store.AuthorAnalyticsSummaryProjection, error) {
	if f.getAuthorAnalyticsSummaryFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorAnalyticsSummaryFn(ctx, pubkey)
}

func (f fakeEventReader) GetAuthorTopicStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]store.AuthorTopicStatsProjection, error) {
	if f.getAuthorTopicStatsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorTopicStatsFn(ctx, pubkey, windowDays, limit)
}

func (f fakeEventReader) GetAuthorRelayFootprint(
	ctx context.Context,
	pubkey string,
	topRelayLimit int,
) (store.AuthorRelayFootprintProjection, error) {
	if f.getAuthorRelayFootprintFn == nil {
		return store.AuthorRelayFootprintProjection{Pubkey: pubkey}, nil
	}
	return f.getAuthorRelayFootprintFn(ctx, pubkey, topRelayLimit)
}

func (f fakeEventReader) GetAuthorMediaMixStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (store.AuthorMediaMixStatsProjection, error) {
	if f.getAuthorMediaMixStatsFn == nil {
		return store.AuthorMediaMixStatsProjection{Pubkey: pubkey, WindowDays: windowDays}, nil
	}
	return f.getAuthorMediaMixStatsFn(ctx, pubkey, windowDays)
}

func (f fakeEventReader) GetAuthorActivityWindowBuckets(
	ctx context.Context,
	pubkey string,
	windowDays int,
) ([]store.AuthorActivityWindowBucketProjection, error) {
	if f.getAuthorActivityWindowBucketsFn == nil {
		return []store.AuthorActivityWindowBucketProjection{}, nil
	}
	return f.getAuthorActivityWindowBucketsFn(ctx, pubkey, windowDays)
}

func (f fakeEventReader) GetAuthorPostingPatternBuckets(
	ctx context.Context,
	pubkey string,
	windowDays int,
) ([]store.AuthorPostingPatternBucketProjection, error) {
	if f.getAuthorPostingPatternBucketsFn == nil {
		return []store.AuthorPostingPatternBucketProjection{}, nil
	}
	return f.getAuthorPostingPatternBucketsFn(ctx, pubkey, windowDays)
}

func (f fakeEventReader) GetAuthorTopNotes(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]store.AuthorTopNoteProjection, error) {
	if f.getAuthorTopNotesFn == nil {
		return []store.AuthorTopNoteProjection{}, nil
	}
	return f.getAuthorTopNotesFn(ctx, pubkey, windowDays, limit)
}

func (f fakeEventReader) GetGroupedNoteAnalytics(
	ctx context.Context,
	query store.GroupedNoteAnalyticsQuery,
) (store.GroupedNoteAnalyticsProjection, error) {
	if f.getGroupedNoteAnalyticsFn == nil {
		return store.GroupedNoteAnalyticsProjection{}, errors.New("not implemented")
	}
	return f.getGroupedNoteAnalyticsFn(ctx, query)
}

func (f fakeEventReader) GetAuthorRecycleCandidates(
	ctx context.Context,
	pubkey string,
	windowDays int,
	minAgeDays int,
	minPerformancePercentile float64,
	includeReplies bool,
	excludeRecentlyReposted bool,
	recentRepostWindowDays int,
	limit int,
) ([]store.AuthorRecycleCandidateProjection, error) {
	if f.getAuthorRecycleCandidatesFn == nil {
		return []store.AuthorRecycleCandidateProjection{}, nil
	}
	return f.getAuthorRecycleCandidatesFn(
		ctx,
		pubkey,
		windowDays,
		minAgeDays,
		minPerformancePercentile,
		includeReplies,
		excludeRecentlyReposted,
		recentRepostWindowDays,
		limit,
	)
}

func (f fakeEventReader) GetAuthorPerformanceAggregate(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (store.AuthorPerformanceAggregateProjection, store.AuthorPerformanceAggregateProjection, error) {
	if f.getAuthorPerformanceAggregateFn == nil {
		return store.AuthorPerformanceAggregateProjection{}, store.AuthorPerformanceAggregateProjection{}, nil
	}
	return f.getAuthorPerformanceAggregateFn(ctx, pubkey, windowDays)
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

func (f fakeEventReader) GetThreadSummary(ctx context.Context, rootEventID string) (store.ThreadSummaryProjection, error) {
	if f.getThreadSummaryFn == nil {
		return store.ThreadSummaryProjection{}, errors.New("not implemented")
	}
	return f.getThreadSummaryFn(ctx, rootEventID)
}

func (f fakeEventReader) ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	if f.listRelayHealthFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.listRelayHealthFn(ctx)
}

func (f fakeEventReader) GetContactListByPubkey(ctx context.Context, pubkey string) (storeread.ContactListProjection, error) {
	if f.getContactListFn == nil {
		return storeread.ContactListProjection{}, errors.New("not implemented")
	}
	return f.getContactListFn(ctx, pubkey)
}

func (f fakeEventReader) GetRelayListByPubkey(ctx context.Context, pubkey string) (storeread.RelayListProjection, error) {
	if f.getRelayListFn == nil {
		return storeread.RelayListProjection{}, errors.New("not implemented")
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
	language string,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if f.searchNotesFn != nil {
		return f.searchNotesFn(ctx, query, sort, window, language, limit, offset)
	}
	if f.searchEventsFn != nil && sort == "relevant" && window == nil && offset == 0 && strings.TrimSpace(language) == "" {
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

func (f fakeEventReader) SuggestHashtags(ctx context.Context, query string, limit int) ([]storeread.TrendingHashtag, error) {
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

func (f fakeEventReader) GetTrustScore(ctx context.Context, pubkey string) (storetrust.TrustGlobalScore, error) {
	if f.getTrustScoreFn == nil {
		return storetrust.TrustGlobalScore{}, errors.New("not implemented")
	}
	return f.getTrustScoreFn(ctx, pubkey)
}

func (f fakeEventReader) GetTrustState(ctx context.Context, pubkey string) (storetrust.TrustState, error) {
	return storetrust.TrustState{}, errors.New("not implemented")
}

func (f fakeEventReader) GetTrustStates(ctx context.Context, pubkeys []string) (map[string]storetrust.TrustState, error) {
	return nil, errors.New("not implemented")
}

func (f fakeEventReader) ListTopTrustedPubkeys(ctx context.Context, limit int) ([]storetrust.TrustGlobalScore, error) {
	if f.listTopTrustFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.listTopTrustFn(ctx, limit)
}

func (f fakeEventReader) GetTrustRun(ctx context.Context, runID int64) (storetrust.TrustRun, error) {
	if f.getTrustRunFn == nil {
		return storetrust.TrustRun{}, errors.New("not implemented")
	}
	return f.getTrustRunFn(ctx, runID)
}

func (f fakeEventReader) ListTrustRuns(ctx context.Context, limit int) ([]storetrust.TrustRun, error) {
	if f.listTrustRunsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.listTrustRunsFn(ctx, limit)
}

func (f fakeEventReader) GetNetworkStats(ctx context.Context) (storeread.NetworkStats, error) {
	if f.getNetworkStatsFn == nil {
		return storeread.NetworkStats{}, errors.New("not implemented")
	}
	return f.getNetworkStatsFn(ctx)
}

func (f fakeEventReader) GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (storeread.PublicDiscoveryNetworkStats, error) {
	if f.getPublicNetworkStatsFn == nil {
		return storeread.PublicDiscoveryNetworkStats{}, errors.New("not implemented")
	}
	return f.getPublicNetworkStatsFn(ctx, hashtagLimit)
}

func (f fakeEventReader) GetDiscoveryStatsSeries(ctx context.Context, metric string, window time.Duration) (storeread.DiscoveryStatsSeries, error) {
	if f.getDiscoveryStatsSeriesFn == nil {
		return storeread.DiscoveryStatsSeries{}, errors.New("not implemented")
	}
	return f.getDiscoveryStatsSeriesFn(ctx, metric, window)
}

func (f fakeEventReader) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]storeread.CuratedRecommendedRead, error) {
	if f.getCuratedReadsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getCuratedReadsFn(ctx, limit)
}

func (f fakeEventReader) GetCuratedReadsTopics(ctx context.Context, limit int) ([]storeread.CuratedReadsTopic, error) {
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
) ([]storeread.TrendingHashtag, error) {
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
) ([]storeread.TrendingNote, error) {
	if f.getTrendingNotesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getTrendingNotesFn(ctx, window, limit, offset)
}

func (f fakeEventReader) GetHotConversations(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]store.HotConversation, error) {
	if f.getHotConversationsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getHotConversationsFn(ctx, window, limit, offset)
}

func (f fakeEventReader) GetHashtagSummary(ctx context.Context, hashtag string) (storeread.HashtagSummary, error) {
	if f.getHashtagSummaryFn == nil {
		return storeread.HashtagSummary{}, errors.New("not implemented")
	}
	return f.getHashtagSummaryFn(ctx, hashtag)
}

func (f fakeEventReader) GetHashtagNotes(
	ctx context.Context,
	hashtag string,
	sort string,
	window string,
	limit int,
	offset int,
) ([]storeread.TrendingNote, error) {
	if f.getHashtagNotesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getHashtagNotesFn(ctx, hashtag, sort, window, limit, offset)
}

func (f fakeEventReader) GetRelatedHashtags(ctx context.Context, hashtag string, limit int) ([]storeread.RelatedHashtag, error) {
	if f.getRelatedHashtagsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getRelatedHashtagsFn(ctx, hashtag, limit)
}

func (f fakeEventReader) GetTrendingDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]store.DomainSummaryProjection, error) {
	if f.getTrendingDomainsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getTrendingDomainsFn(ctx, window, limit, offset)
}

func (f fakeEventReader) GetHomeTrendingDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
) ([]store.DomainSummaryProjection, error) {
	if f.getHomeTrendingDomainsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getHomeTrendingDomainsFn(ctx, window, limit)
}

func (f fakeEventReader) GetDomainSummary(
	ctx context.Context,
	domain string,
	recentLimit int,
	topLimit int,
) (store.DomainSummaryProjection, error) {
	if f.getDomainSummaryFn == nil {
		return store.DomainSummaryProjection{}, errors.New("not implemented")
	}
	return f.getDomainSummaryFn(ctx, domain, recentLimit, topLimit)
}

func (f fakeEventReader) GetDomainNotes(
	ctx context.Context,
	domain string,
	sort string,
	window string,
	limit int,
	offset int,
) ([]storeread.TrendingNote, error) {
	if f.getDomainNotesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getDomainNotesFn(ctx, domain, sort, window, limit, offset)
}

func (f fakeEventReader) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]storeread.CuratedFeaturedAuthor, error) {
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
) ([]storeread.TrendingProfile, error) {
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
) ([]storeread.TrendingProfile, error) {
	if f.getRisingProfilesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getRisingProfilesFn(ctx, window, limit, offset)
}

func (f fakeEventReader) GetRelatedProfiles(
	ctx context.Context,
	pubkey string,
	limit int,
) ([]storeread.RelatedProfile, error) {
	if f.getRelatedProfilesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getRelatedProfilesFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetNoteStats(ctx context.Context, eventID string) (storeread.NoteStats, error) {
	if f.getNoteStatsFn == nil {
		return storeread.NoteStats{}, errors.New("not implemented")
	}
	return f.getNoteStatsFn(ctx, eventID)
}

func (f fakeEventReader) GetNoteConversationVelocity(ctx context.Context, eventID string) (storeread.NoteConversationVelocity, error) {
	if f.getNoteConversationVelocityFn == nil {
		return storeread.NoteConversationVelocity{}, errors.New("not implemented")
	}
	return f.getNoteConversationVelocityFn(ctx, eventID)
}

func (f fakeEventReader) GetNoteQuoteRepostLinkage(
	ctx context.Context,
	eventID string,
	recentLimit int,
) (store.NoteQuoteRepostLinkageProjection, error) {
	if f.getNoteQuoteRepostLinkageFn == nil {
		return store.NoteQuoteRepostLinkageProjection{EventID: eventID}, nil
	}
	return f.getNoteQuoteRepostLinkageFn(ctx, eventID, recentLimit)
}

func (f fakeEventReader) GetRelatedNotes(ctx context.Context, eventID string, limit int) ([]storeread.RelatedNote, error) {
	if f.getRelatedNotesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getRelatedNotesFn(ctx, eventID, limit)
}

func (f trustQualifiedFakeReader) GetTrustQualifications(
	ctx context.Context,
	pubkeys []string,
	policy storetrust.TrustQualificationPolicy,
) (map[string]storetrust.TrustQualification, error) {
	if f.getTrustQualificationsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getTrustQualificationsFn(ctx, pubkeys, policy)
}

func (f trustQualifiedFakeReader) IsTrustedAuthor(
	ctx context.Context,
	pubkey string,
	policy storetrust.TrustQualificationPolicy,
) (bool, error) {
	if f.isTrustedAuthorFn == nil {
		return false, errors.New("not implemented")
	}
	return f.isTrustedAuthorFn(ctx, pubkey, policy)
}
