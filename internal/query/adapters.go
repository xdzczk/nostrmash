package query

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

type legacyReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventWithProvenance(ctx context.Context, id string) (store.EventWithProvenance, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error)
	GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error)
	GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error)
	GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (store.ProfilePublicStatsProjection, error)
	GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error)
	GetEventReplies(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
	ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error)
	GetContactListByPubkey(ctx context.Context, pubkey string) (store.ContactListProjection, error)
	GetRelayListByPubkey(ctx context.Context, pubkey string) (store.RelayListProjection, error)
	SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error)
	SearchProfiles(ctx context.Context, query string, limit int) ([]store.ProfileProjection, error)
	GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
	GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
}

type legacyNotesSearchReader interface {
	SearchNotes(
		ctx context.Context,
		query string,
		sort string,
		window *time.Duration,
		language string,
		limit int,
		offset int,
	) ([]json.RawMessage, error)
}

type legacyProfilesSearchReader interface {
	SearchProfilesWithOptions(
		ctx context.Context,
		query string,
		sort string,
		limit int,
		offset int,
	) ([]store.ProfileProjection, error)
}

type legacySearchSuggestionsReader interface {
	SuggestProfiles(ctx context.Context, query string, limit int) ([]store.ProfileProjection, error)
	SuggestHashtags(ctx context.Context, query string, limit int) ([]store.TrendingHashtag, error)
}

type legacyAuthorAnalyticsSummaryReader interface {
	GetAuthorAnalyticsSummary(ctx context.Context, pubkey string) ([]store.AuthorAnalyticsSummaryProjection, error)
}

type legacyAuthorQuoteRepostRecentActivityReader interface {
	GetAuthorQuoteRepostRecentActivity(ctx context.Context, pubkey string, limit int) ([]store.QuoteRepostActivityProjection, error)
}

type legacyAuthorTopicStatsReader interface {
	GetAuthorTopicStats(ctx context.Context, pubkey string, windowDays int, limit int) ([]store.AuthorTopicStatsProjection, error)
}

type legacyAuthorTopLanguagesReader interface {
	GetAuthorTopLanguages(ctx context.Context, pubkey string, windowDays int, limit int) ([]store.LanguageSummary, error)
}

type legacyAuthorRelayFootprintReader interface {
	GetAuthorRelayFootprint(ctx context.Context, pubkey string, topRelayLimit int) (store.AuthorRelayFootprintProjection, error)
}

type legacyAuthorMediaMixStatsReader interface {
	GetAuthorMediaMixStats(ctx context.Context, pubkey string, windowDays int) (store.AuthorMediaMixStatsProjection, error)
}

type legacyAuthorActivityWindowsReader interface {
	GetAuthorActivityWindowBuckets(ctx context.Context, pubkey string, windowDays int) ([]store.AuthorActivityWindowBucketProjection, error)
}

type legacyAuthorPostingPatternsReader interface {
	GetAuthorPostingPatternBuckets(ctx context.Context, pubkey string, windowDays int) ([]store.AuthorPostingPatternBucketProjection, error)
}

type legacyAuthorTopNotesReader interface {
	GetAuthorTopNotes(ctx context.Context, pubkey string, windowDays int, limit int) ([]store.AuthorTopNoteProjection, error)
}

type legacyAuthorRecycleCandidatesReader interface {
	GetAuthorRecycleCandidates(
		ctx context.Context,
		pubkey string,
		windowDays int,
		minAgeDays int,
		minPerformancePercentile float64,
		includeReplies bool,
		excludeRecentlyReposted bool,
		recentRepostWindowDays int,
		limit int,
	) ([]store.AuthorRecycleCandidateProjection, error)
}

type legacyAuthorPerformanceAggregateReader interface {
	GetAuthorPerformanceAggregate(
		ctx context.Context,
		pubkey string,
		windowDays int,
	) (store.AuthorPerformanceAggregateProjection, store.AuthorPerformanceAggregateProjection, error)
	GetAuthorMediaMixStats(ctx context.Context, pubkey string, windowDays int) (store.AuthorMediaMixStatsProjection, error)
	GetAuthorTopicStats(ctx context.Context, pubkey string, windowDays int, limit int) ([]store.AuthorTopicStatsProjection, error)
}

type legacyReaderAdapter struct {
	legacy legacyReader
}

type legacyDescendingThreadWindowReader interface {
	GetEventRepliesDescending(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor, offset int) ([]json.RawMessage, *store.EventOrderCursor, error)
}

func (a legacyReaderAdapter) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	return a.legacy.GetEventRawByID(ctx, id)
}

func (a legacyReaderAdapter) GetEventWithProvenance(ctx context.Context, id string) (EventWithProvenance, error) {
	row, err := a.legacy.GetEventWithProvenance(ctx, id)
	if err != nil {
		return EventWithProvenance{}, err
	}
	return eventWithProvenanceFromStore(row), nil
}

func (a legacyReaderAdapter) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return a.legacy.GetEventRawsByIDs(ctx, ids)
}

func (a legacyReaderAdapter) GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error) {
	return a.legacy.GetEventSeenOn(ctx, id)
}

func (a legacyReaderAdapter) GetProfileByPubkey(ctx context.Context, pubkey string) (Profile, error) {
	row, err := a.legacy.GetProfileByPubkey(ctx, pubkey)
	if err != nil {
		return Profile{}, err
	}
	return profileFromStore(row), nil
}

func (a legacyReaderAdapter) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	rows, err := a.legacy.GetProfilesByPubkeys(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Profile, len(rows))
	for pubkey, row := range rows {
		out[pubkey] = profileFromStore(row)
	}
	return out, nil
}

func (a legacyReaderAdapter) GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (ProfilePublicStats, error) {
	row, err := a.legacy.GetProfilePublicStatsByPubkey(ctx, pubkey)
	if err != nil {
		return ProfilePublicStats{}, err
	}
	return profilePublicStatsFromStore(row), nil
}

func (a legacyReaderAdapter) GetAuthorAnalyticsSummary(ctx context.Context, pubkey string) (AuthorAnalyticsSummary, error) {
	reader, ok := a.legacy.(legacyAuthorAnalyticsSummaryReader)
	if !ok {
		return AuthorAnalyticsSummary{}, unsupportedCapabilityError("author analytics summary")
	}
	rows, err := reader.GetAuthorAnalyticsSummary(ctx, pubkey)
	if err != nil {
		return AuthorAnalyticsSummary{}, err
	}
	out := authorAnalyticsSummaryFromStore(pubkey, rows)
	recent, err := a.GetAuthorQuoteRepostRecentActivity(ctx, pubkey, 8)
	if err == nil {
		out.RecentQuoteRepostActivity = recent
	}
	if relayReader, ok := a.legacy.(legacyAuthorRelayFootprintReader); ok {
		relayFootprint, relayErr := relayReader.GetAuthorRelayFootprint(ctx, pubkey, 8)
		if relayErr == nil {
			mapped := authorRelayFootprintFromStore(relayFootprint)
			out.RelayFootprint = &mapped
		}
	}
	return out, nil
}

func (a legacyReaderAdapter) GetAuthorQuoteRepostRecentActivity(
	ctx context.Context,
	pubkey string,
	limit int,
) ([]QuoteRepostActivity, error) {
	reader, ok := a.legacy.(legacyAuthorQuoteRepostRecentActivityReader)
	if !ok {
		return nil, unsupportedCapabilityError("author quote/repost recent activity")
	}
	rows, err := reader.GetAuthorQuoteRepostRecentActivity(ctx, pubkey, limit)
	if err != nil {
		return nil, err
	}
	out := make([]QuoteRepostActivity, 0, len(rows))
	for _, row := range rows {
		out = append(out, quoteRepostActivityFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) GetAuthorTopicStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]AuthorTopicStat, error) {
	reader, ok := a.legacy.(legacyAuthorTopicStatsReader)
	if !ok {
		return nil, unsupportedCapabilityError("author topic stats")
	}
	rows, err := reader.GetAuthorTopicStats(ctx, pubkey, windowDays, limit)
	if err != nil {
		return nil, err
	}
	out := make([]AuthorTopicStat, 0, len(rows))
	for _, row := range rows {
		out = append(out, authorTopicStatFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) GetAuthorTopLanguages(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]LanguageSummary, error) {
	reader, ok := a.legacy.(legacyAuthorTopLanguagesReader)
	if !ok {
		return nil, unsupportedCapabilityError("author top languages")
	}
	rows, err := reader.GetAuthorTopLanguages(ctx, pubkey, windowDays, limit)
	if err != nil {
		return nil, err
	}
	out := make([]LanguageSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, languageSummaryFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) GetAuthorMediaMixStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorAnalyticsMediaMix, error) {
	reader, ok := a.legacy.(legacyAuthorMediaMixStatsReader)
	if !ok {
		return AuthorAnalyticsMediaMix{}, unsupportedCapabilityError("author media mix stats")
	}
	row, err := reader.GetAuthorMediaMixStats(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorAnalyticsMediaMix{}, err
	}
	return authorMediaMixFromStore(row), nil
}

func (a legacyReaderAdapter) GetAuthorActivityWindows(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorActivityWindows, error) {
	reader, ok := a.legacy.(legacyAuthorActivityWindowsReader)
	if !ok {
		return AuthorActivityWindows{}, unsupportedCapabilityError("author activity windows")
	}
	rows, err := reader.GetAuthorActivityWindowBuckets(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorActivityWindows{}, err
	}
	return authorActivityWindowsFromStore(pubkey, windowDays, rows), nil
}

func (a legacyReaderAdapter) GetAuthorPostingPatterns(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorPostingPatterns, error) {
	reader, ok := a.legacy.(legacyAuthorPostingPatternsReader)
	if !ok {
		return AuthorPostingPatterns{}, unsupportedCapabilityError("author posting patterns")
	}
	rows, err := reader.GetAuthorPostingPatternBuckets(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorPostingPatterns{}, err
	}
	return authorPostingPatternsFromStore(pubkey, windowDays, rows), nil
}

func (a legacyReaderAdapter) GetAuthorTopNotes(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]AuthorTopNote, error) {
	reader, ok := a.legacy.(legacyAuthorTopNotesReader)
	if !ok {
		return nil, unsupportedCapabilityError("author top notes")
	}
	rows, err := reader.GetAuthorTopNotes(ctx, pubkey, windowDays, limit)
	if err != nil {
		return nil, err
	}
	out := make([]AuthorTopNote, 0, len(rows))
	for _, row := range rows {
		out = append(out, authorTopNoteFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) GetAuthorRecycleCandidates(
	ctx context.Context,
	pubkey string,
	windowDays int,
	minAgeDays int,
	minPerformancePercentile float64,
	includeReplies bool,
	excludeRecentlyReposted bool,
	recentRepostWindowDays int,
	limit int,
) ([]AuthorRecycleCandidate, error) {
	reader, ok := a.legacy.(legacyAuthorRecycleCandidatesReader)
	if !ok {
		return nil, unsupportedCapabilityError("author recycle candidates")
	}
	rows, err := reader.GetAuthorRecycleCandidates(
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
	if err != nil {
		return nil, err
	}
	out := make([]AuthorRecycleCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, authorRecycleCandidateFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) GetAuthorPerformanceSummary(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorPerformanceSummary, error) {
	reader, ok := a.legacy.(legacyAuthorPerformanceAggregateReader)
	if !ok {
		return AuthorPerformanceSummary{}, unsupportedCapabilityError("author performance summary")
	}
	current, previous, err := reader.GetAuthorPerformanceAggregate(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorPerformanceSummary{}, err
	}
	mediaMix, err := reader.GetAuthorMediaMixStats(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorPerformanceSummary{}, err
	}
	topics, err := reader.GetAuthorTopicStats(ctx, pubkey, windowDays, 5)
	if err != nil {
		return AuthorPerformanceSummary{}, err
	}
	return authorPerformanceSummaryFromStore(pubkey, windowDays, current, previous, mediaMix, topics), nil
}

func (a legacyReaderAdapter) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetAuthorRecentEvents(ctx, pubkey, limit)
}

func (a legacyReaderAdapter) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetAuthorReplies(ctx, pubkey, limit)
}

func (a legacyReaderAdapter) GetEventCounts(ctx context.Context, eventID string) (EventCounts, error) {
	row, err := a.legacy.GetEventCounts(ctx, eventID)
	if err != nil {
		return EventCounts{}, err
	}
	return eventCountsFromStore(row), nil
}

func (a legacyReaderAdapter) GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) ([]json.RawMessage, *EventCursor, error) {
	replies, next, err := a.legacy.GetEventReplies(ctx, eventID, limit, eventCursorToStore(cursor))
	if err != nil {
		return nil, nil, err
	}
	return replies, eventCursorFromStore(next), nil
}

func (a legacyReaderAdapter) GetEventRepliesDescending(ctx context.Context, eventID string, limit int, cursor *EventCursor, offset int) ([]json.RawMessage, *EventCursor, error) {
	descReader, ok := a.legacy.(legacyDescendingThreadWindowReader)
	if !ok {
		return nil, nil, unsupportedCapabilityError("thread descending replies")
	}
	replies, next, err := descReader.GetEventRepliesDescending(ctx, eventID, limit, eventCursorToStore(cursor), offset)
	if err != nil {
		return nil, nil, err
	}
	return replies, eventCursorFromStore(next), nil
}

func (a legacyReaderAdapter) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
	return a.legacy.GetEventAncestors(ctx, eventID, maxDepth)
}

func (a legacyReaderAdapter) ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	return a.legacy.ListRelayHealth(ctx)
}

func (a legacyReaderAdapter) GetContactListByPubkey(ctx context.Context, pubkey string) (ContactList, error) {
	row, err := a.legacy.GetContactListByPubkey(ctx, pubkey)
	if err != nil {
		return ContactList{}, err
	}
	return contactListFromStore(row), nil
}

func (a legacyReaderAdapter) GetRelayListByPubkey(ctx context.Context, pubkey string) (RelayList, error) {
	row, err := a.legacy.GetRelayListByPubkey(ctx, pubkey)
	if err != nil {
		return RelayList{}, err
	}
	return relayListFromStore(row), nil
}

func (a legacyReaderAdapter) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	return a.legacy.SearchEventsByContent(ctx, query, limit)
}

func (a legacyReaderAdapter) SearchProfiles(ctx context.Context, query string, limit int) ([]Profile, error) {
	rows, err := a.legacy.SearchProfiles(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Profile, 0, len(rows))
	for _, row := range rows {
		out = append(out, profileFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) SearchNotes(
	ctx context.Context,
	query string,
	sort string,
	window *time.Duration,
	language string,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if advanced, ok := a.legacy.(legacyNotesSearchReader); ok {
		return advanced.SearchNotes(ctx, query, sort, window, language, limit, offset)
	}
	if sort == "relevant" && window == nil && offset == 0 && language == "" {
		return a.legacy.SearchEventsByContent(ctx, query, limit)
	}
	return nil, unsupportedCapabilityError("advanced notes search")
}

func (a legacyReaderAdapter) SearchProfilesWithOptions(
	ctx context.Context,
	query string,
	sort string,
	limit int,
	offset int,
) ([]Profile, error) {
	if advanced, ok := a.legacy.(legacyProfilesSearchReader); ok {
		rows, err := advanced.SearchProfilesWithOptions(ctx, query, sort, limit, offset)
		if err != nil {
			return nil, err
		}
		out := make([]Profile, 0, len(rows))
		for _, row := range rows {
			out = append(out, profileFromStore(row))
		}
		return out, nil
	}
	if sort == "relevant" && offset == 0 {
		return a.SearchProfiles(ctx, query, limit)
	}
	return nil, unsupportedCapabilityError("advanced profile search")
}

func (a legacyReaderAdapter) SuggestProfiles(ctx context.Context, query string, limit int) ([]Profile, error) {
	reader, ok := a.legacy.(legacySearchSuggestionsReader)
	if !ok {
		return nil, unsupportedCapabilityError("search profile suggestions")
	}
	rows, err := reader.SuggestProfiles(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Profile, 0, len(rows))
	for _, row := range rows {
		out = append(out, profileFromStore(row))
	}
	return out, nil
}

func (a legacyReaderAdapter) SuggestHashtags(ctx context.Context, query string, limit int) ([]HashtagSuggestion, error) {
	reader, ok := a.legacy.(legacySearchSuggestionsReader)
	if !ok {
		return nil, unsupportedCapabilityError("search hashtag suggestions")
	}
	rows, err := reader.SuggestHashtags(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]HashtagSuggestion, 0, len(rows))
	for _, row := range rows {
		out = append(out, HashtagSuggestion{
			Hashtag:       row.Hashtag,
			EventCount:    row.EventCount,
			UniqueAuthors: row.UniqueAuthors,
		})
	}
	return out, nil
}

func (a legacyReaderAdapter) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
}

func (a legacyReaderAdapter) GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetEventsReferencingPubkey(ctx, targetPubkey, limit)
}

func (a legacyReaderAdapter) GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	return a.legacy.GetFollowersByPubkey(ctx, targetPubkey, limit)
}

type legacyFallbackReader interface {
	FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error)
}

type legacyFallbackReaderAdapter struct {
	legacy legacyFallbackReader
}

func adaptReader(reader any) (Reader, error) {
	if adapted, ok := reader.(Reader); ok {
		return adapted, nil
	}
	if legacy, ok := reader.(legacyReader); ok {
		return legacyReaderAdapter{legacy: legacy}, nil
	}
	return nil, fmt.Errorf("query: unsupported reader type %T", reader)
}

func adaptFallbackReader(reader any) (FallbackReader, error) {
	if reader == nil {
		return nil, nil
	}
	if adapted, ok := reader.(FallbackReader); ok {
		return adapted, nil
	}
	if legacy, ok := reader.(legacyFallbackReader); ok {
		return legacyFallbackReaderAdapter{legacy: legacy}, nil
	}
	return nil, fmt.Errorf("query: unsupported fallback reader type %T", reader)
}

func (a legacyFallbackReaderAdapter) FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return a.legacy.FetchEventsByIDs(ctx, ids)
}

func (a legacyFallbackReaderAdapter) FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	rows, err := a.legacy.FetchProfilesByPubkeys(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Profile, len(rows))
	for pubkey, row := range rows {
		out[pubkey] = profileFromStore(row)
	}
	return out, nil
}
