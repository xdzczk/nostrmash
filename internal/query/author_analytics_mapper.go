package query

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type readModelAuthorAnalyticsSummaryReader interface {
	GetAuthorAnalyticsSummary(ctx context.Context, pubkey string) ([]readmodel.AuthorAnalyticsSummaryProjection, error)
}

type readModelAuthorQuoteRepostRecentActivityReader interface {
	GetAuthorQuoteRepostRecentActivity(ctx context.Context, pubkey string, limit int) ([]readmodel.QuoteRepostActivityProjection, error)
}

type readModelAuthorTopicStatsReader interface {
	GetAuthorTopicStats(ctx context.Context, pubkey string, windowDays int, limit int) ([]readmodel.AuthorTopicStatsProjection, error)
}

type readModelAuthorTopLanguagesReader interface {
	GetAuthorTopLanguages(ctx context.Context, pubkey string, windowDays int, limit int) ([]readmodel.LanguageSummary, error)
}

type readModelAuthorRelayFootprintReader interface {
	GetAuthorRelayFootprint(ctx context.Context, pubkey string, topRelayLimit int) (readmodel.AuthorRelayFootprintProjection, error)
}

type readModelAuthorMediaMixStatsReader interface {
	GetAuthorMediaMixStats(ctx context.Context, pubkey string, windowDays int) (readmodel.AuthorMediaMixStatsProjection, error)
}

type readModelAuthorActivityWindowsReader interface {
	GetAuthorActivityWindowBuckets(ctx context.Context, pubkey string, windowDays int) ([]readmodel.AuthorActivityWindowBucketProjection, error)
}

type readModelAuthorPostingPatternsReader interface {
	GetAuthorPostingPatternBuckets(ctx context.Context, pubkey string, windowDays int) ([]readmodel.AuthorPostingPatternBucketProjection, error)
}

type readModelAuthorTopNotesReader interface {
	GetAuthorTopNotes(ctx context.Context, pubkey string, windowDays int, limit int) ([]readmodel.AuthorTopNoteProjection, error)
}

type readModelAuthorRecycleCandidatesReader interface {
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
	) ([]readmodel.AuthorRecycleCandidateProjection, error)
}

type readModelAuthorPerformanceAggregateReader interface {
	GetAuthorPerformanceAggregate(
		ctx context.Context,
		pubkey string,
		windowDays int,
	) (readmodel.AuthorPerformanceAggregateProjection, readmodel.AuthorPerformanceAggregateProjection, error)
	GetAuthorMediaMixStats(ctx context.Context, pubkey string, windowDays int) (readmodel.AuthorMediaMixStatsProjection, error)
	GetAuthorTopicStats(ctx context.Context, pubkey string, windowDays int, limit int) ([]readmodel.AuthorTopicStatsProjection, error)
}

func (a readModelReaderAdapter) GetAuthorAnalyticsSummary(ctx context.Context, pubkey string) (AuthorAnalyticsSummary, error) {
	reader, ok := a.readModel.(readModelAuthorAnalyticsSummaryReader)
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
	if relayReader, ok := a.readModel.(readModelAuthorRelayFootprintReader); ok {
		relayFootprint, relayErr := relayReader.GetAuthorRelayFootprint(ctx, pubkey, 8)
		if relayErr == nil {
			mapped := authorRelayFootprintFromStore(relayFootprint)
			out.RelayFootprint = &mapped
		}
	}
	return out, nil
}

func (a readModelReaderAdapter) GetAuthorQuoteRepostRecentActivity(
	ctx context.Context,
	pubkey string,
	limit int,
) ([]QuoteRepostActivity, error) {
	reader, ok := a.readModel.(readModelAuthorQuoteRepostRecentActivityReader)
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

func (a readModelReaderAdapter) GetAuthorTopicStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]AuthorTopicStat, error) {
	reader, ok := a.readModel.(readModelAuthorTopicStatsReader)
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

func (a readModelReaderAdapter) GetAuthorTopLanguages(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]LanguageSummary, error) {
	reader, ok := a.readModel.(readModelAuthorTopLanguagesReader)
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

func (a readModelReaderAdapter) GetAuthorMediaMixStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorAnalyticsMediaMix, error) {
	reader, ok := a.readModel.(readModelAuthorMediaMixStatsReader)
	if !ok {
		return AuthorAnalyticsMediaMix{}, unsupportedCapabilityError("author media mix stats")
	}
	row, err := reader.GetAuthorMediaMixStats(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorAnalyticsMediaMix{}, err
	}
	return authorMediaMixFromStore(row), nil
}

func (a readModelReaderAdapter) GetAuthorActivityWindows(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorActivityWindows, error) {
	reader, ok := a.readModel.(readModelAuthorActivityWindowsReader)
	if !ok {
		return AuthorActivityWindows{}, unsupportedCapabilityError("author activity windows")
	}
	rows, err := reader.GetAuthorActivityWindowBuckets(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorActivityWindows{}, err
	}
	return authorActivityWindowsFromStore(pubkey, windowDays, rows), nil
}

func (a readModelReaderAdapter) GetAuthorPostingPatterns(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorPostingPatterns, error) {
	reader, ok := a.readModel.(readModelAuthorPostingPatternsReader)
	if !ok {
		return AuthorPostingPatterns{}, unsupportedCapabilityError("author posting patterns")
	}
	rows, err := reader.GetAuthorPostingPatternBuckets(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorPostingPatterns{}, err
	}
	return authorPostingPatternsFromStore(pubkey, windowDays, rows), nil
}

func (a readModelReaderAdapter) GetAuthorTopNotes(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]AuthorTopNote, error) {
	reader, ok := a.readModel.(readModelAuthorTopNotesReader)
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

func (a readModelReaderAdapter) GetAuthorRecycleCandidates(
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
	reader, ok := a.readModel.(readModelAuthorRecycleCandidatesReader)
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

func (a readModelReaderAdapter) GetAuthorPerformanceSummary(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorPerformanceSummary, error) {
	reader, ok := a.readModel.(readModelAuthorPerformanceAggregateReader)
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
