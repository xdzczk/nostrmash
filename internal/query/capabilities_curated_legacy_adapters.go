package query

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
)

type legacyNetworkStatsCapability interface {
	GetNetworkStats(ctx context.Context) (store.NetworkStats, error)
}

type legacyPublicDiscoveryNetworkStatsCapability interface {
	GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (store.PublicDiscoveryNetworkStats, error)
}

type legacyNetworkStatsAdapter struct {
	legacy legacyNetworkStatsCapability
}

func (a legacyNetworkStatsAdapter) GetNetworkStats(ctx context.Context) (NetworkStats, error) {
	row, err := a.legacy.GetNetworkStats(ctx)
	if err != nil {
		return NetworkStats{}, err
	}
	return networkStatsFromStore(row), nil
}

type legacyPublicDiscoveryNetworkStatsAdapter struct {
	legacy legacyPublicDiscoveryNetworkStatsCapability
}

func (a legacyPublicDiscoveryNetworkStatsAdapter) GetPublicDiscoveryNetworkStats(
	ctx context.Context,
	hashtagLimit int,
) (PublicDiscoveryNetworkStats, error) {
	row, err := a.legacy.GetPublicDiscoveryNetworkStats(ctx, hashtagLimit)
	if err != nil {
		return PublicDiscoveryNetworkStats{}, err
	}
	return publicDiscoveryNetworkStatsFromStore(row), nil
}

type legacyCuratedRecommendedReadsCapability interface {
	GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error)
}

type legacyCuratedRecommendedReadsAdapter struct {
	legacy legacyCuratedRecommendedReadsCapability
}

func (a legacyCuratedRecommendedReadsAdapter) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error) {
	rows, err := a.legacy.GetCuratedRecommendedReads(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CuratedRecommendedRead, 0, len(rows))
	for _, row := range rows {
		out = append(out, curatedRecommendedReadFromStore(row))
	}
	return out, nil
}

type legacyCuratedReadsTopicsCapability interface {
	GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error)
}

type legacyCuratedReadsTopicsAdapter struct {
	legacy legacyCuratedReadsTopicsCapability
}

func (a legacyCuratedReadsTopicsAdapter) GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error) {
	rows, err := a.legacy.GetCuratedReadsTopics(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CuratedReadsTopic, 0, len(rows))
	for _, row := range rows {
		out = append(out, curatedReadsTopicFromStore(row))
	}
	return out, nil
}

type legacyCuratedFeaturedAuthorsCapability interface {
	GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error)
}

type legacyCuratedFeaturedAuthorsAdapter struct {
	legacy legacyCuratedFeaturedAuthorsCapability
}

func (a legacyCuratedFeaturedAuthorsAdapter) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error) {
	rows, err := a.legacy.GetCuratedFeaturedAuthors(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CuratedFeaturedAuthor, 0, len(rows))
	for _, row := range rows {
		out = append(out, curatedFeaturedAuthorFromStore(row))
	}
	return out, nil
}

type legacyTrendingHashtagsCapability interface {
	GetTrendingHashtags(ctx context.Context, window time.Duration, limit int, offset int) ([]store.TrendingHashtag, error)
}

type legacyTrendingHashtagsAdapter struct {
	legacy legacyTrendingHashtagsCapability
}

func (a legacyTrendingHashtagsAdapter) GetTrendingHashtags(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]TrendingHashtag, error) {
	rows, err := a.legacy.GetTrendingHashtags(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingHashtag, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingHashtagFromStore(row))
	}
	return out, nil
}

type legacyHashtagSummaryCapability interface {
	GetHashtagSummary(ctx context.Context, hashtag string) (store.HashtagSummary, error)
}

type legacyHashtagSummaryAdapter struct {
	legacy legacyHashtagSummaryCapability
}

func (a legacyHashtagSummaryAdapter) GetHashtagSummary(ctx context.Context, hashtag string) (HashtagSummary, error) {
	row, err := a.legacy.GetHashtagSummary(ctx, hashtag)
	if err != nil {
		return HashtagSummary{}, err
	}
	return hashtagSummaryFromStore(row), nil
}

type legacyHashtagNotesCapability interface {
	GetHashtagNotes(ctx context.Context, hashtag string, sort string, window string, limit int, offset int) ([]store.TrendingNote, error)
}

type legacyHashtagNotesAdapter struct {
	legacy legacyHashtagNotesCapability
}

func (a legacyHashtagNotesAdapter) GetHashtagNotes(
	ctx context.Context,
	hashtag string,
	sort string,
	window string,
	limit int,
	offset int,
) ([]TrendingNote, error) {
	rows, err := a.legacy.GetHashtagNotes(ctx, hashtag, sort, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingNote, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingNoteFromStore(row))
	}
	return out, nil
}

type legacyRelatedHashtagsCapability interface {
	GetRelatedHashtags(ctx context.Context, hashtag string, limit int) ([]store.RelatedHashtag, error)
}

type legacyEventLinkedDomainsCapability interface {
	GetEventLinkedDomains(ctx context.Context, eventID string, limit int) ([]store.EventDomainLinkProjection, error)
}

type legacyTopDomainsCapability interface {
	GetTopDomains(ctx context.Context, window time.Duration, limit int, offset int) ([]store.DomainStatProjection, error)
}

type legacyTopDomainsByAuthorCapability interface {
	GetTopDomainsByAuthor(ctx context.Context, pubkey string, window time.Duration, limit int, offset int) ([]store.DomainStatProjection, error)
}

type legacyTrendingDomainsCapability interface {
	GetTrendingDomains(ctx context.Context, window time.Duration, limit int, offset int) ([]store.DomainSummaryProjection, error)
}

type legacyDomainSummaryCapability interface {
	GetDomainSummary(ctx context.Context, domain string, recentLimit int, topLimit int) (store.DomainSummaryProjection, error)
}

type legacyDomainNotesCapability interface {
	GetDomainNotes(ctx context.Context, domain string, sort string, window string, limit int, offset int) ([]store.TrendingNote, error)
}

type legacyRelatedHashtagsAdapter struct {
	legacy legacyRelatedHashtagsCapability
}

func (a legacyRelatedHashtagsAdapter) GetRelatedHashtags(ctx context.Context, hashtag string, limit int) ([]RelatedHashtag, error) {
	rows, err := a.legacy.GetRelatedHashtags(ctx, hashtag, limit)
	if err != nil {
		return nil, err
	}
	out := make([]RelatedHashtag, 0, len(rows))
	for _, row := range rows {
		out = append(out, relatedHashtagFromStore(row))
	}
	return out, nil
}

type legacyEventLinkedDomainsAdapter struct {
	legacy legacyEventLinkedDomainsCapability
}

func (a legacyEventLinkedDomainsAdapter) GetEventLinkedDomains(
	ctx context.Context,
	eventID string,
	limit int,
) ([]EventDomainLink, error) {
	rows, err := a.legacy.GetEventLinkedDomains(ctx, eventID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]EventDomainLink, 0, len(rows))
	for _, row := range rows {
		out = append(out, eventDomainLinkFromStore(row))
	}
	return out, nil
}

type legacyTopDomainsAdapter struct {
	legacy legacyTopDomainsCapability
}

func (a legacyTopDomainsAdapter) GetTopDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]DomainStat, error) {
	rows, err := a.legacy.GetTopDomains(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]DomainStat, 0, len(rows))
	for _, row := range rows {
		out = append(out, domainStatFromStore(row))
	}
	return out, nil
}

type legacyTopDomainsByAuthorAdapter struct {
	legacy legacyTopDomainsByAuthorCapability
}

func (a legacyTopDomainsByAuthorAdapter) GetTopDomainsByAuthor(
	ctx context.Context,
	pubkey string,
	window time.Duration,
	limit int,
	offset int,
) ([]DomainStat, error) {
	rows, err := a.legacy.GetTopDomainsByAuthor(ctx, pubkey, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]DomainStat, 0, len(rows))
	for _, row := range rows {
		out = append(out, domainStatFromStore(row))
	}
	return out, nil
}

type legacyTrendingDomainsAdapter struct {
	legacy legacyTrendingDomainsCapability
}

func (a legacyTrendingDomainsAdapter) GetTrendingDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]DomainSummary, error) {
	rows, err := a.legacy.GetTrendingDomains(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]DomainSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, domainSummaryFromStore(row))
	}
	return out, nil
}

type legacyDomainSummaryAdapter struct {
	legacy legacyDomainSummaryCapability
}

func (a legacyDomainSummaryAdapter) GetDomainSummary(
	ctx context.Context,
	domain string,
	recentLimit int,
	topLimit int,
) (DomainSummary, error) {
	row, err := a.legacy.GetDomainSummary(ctx, domain, recentLimit, topLimit)
	if err != nil {
		return DomainSummary{}, err
	}
	return domainSummaryFromStore(row), nil
}

type legacyDomainNotesAdapter struct {
	legacy legacyDomainNotesCapability
}

func (a legacyDomainNotesAdapter) GetDomainNotes(
	ctx context.Context,
	domain string,
	sort string,
	window string,
	limit int,
	offset int,
) ([]TrendingNote, error) {
	rows, err := a.legacy.GetDomainNotes(ctx, domain, sort, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingNote, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingNoteFromStore(row))
	}
	return out, nil
}

type legacyTrendingNotesCapability interface {
	GetTrendingNotes(ctx context.Context, window time.Duration, limit int, offset int) ([]store.TrendingNote, error)
}

type legacyHotConversationsCapability interface {
	GetHotConversations(ctx context.Context, window time.Duration, limit int, offset int) ([]store.HotConversation, error)
}

type legacyTrustQualifiedTrendingNotesCapability interface {
	GetTrustQualifiedTrendingNotes(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		mode string,
		policy store.TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]store.TrustQualifiedTrendingNote, bool, error)
}

type legacyTrendingNotesAdapter struct {
	legacy legacyTrendingNotesCapability
}

type legacyHotConversationsAdapter struct {
	legacy legacyHotConversationsCapability
}

func (a legacyTrendingNotesAdapter) GetTrendingNotes(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]TrendingNote, error) {
	rows, err := a.legacy.GetTrendingNotes(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingNote, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingNoteFromStore(row))
	}
	return out, nil
}

func (a legacyHotConversationsAdapter) GetHotConversations(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]HotConversation, error) {
	rows, err := a.legacy.GetHotConversations(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]HotConversation, 0, len(rows))
	for _, row := range rows {
		out = append(out, hotConversationFromStore(row))
	}
	return out, nil
}

type legacyTrustQualifiedTrendingNotesAdapter struct {
	legacy legacyTrustQualifiedTrendingNotesCapability
}

func (a legacyTrustQualifiedTrendingNotesAdapter) GetTrustQualifiedTrendingNotes(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	mode string,
	policy TrustQualificationPolicy,
	maxStaleness time.Duration,
) ([]trustedNoteCandidate, bool, error) {
	rows, ready, err := a.legacy.GetTrustQualifiedTrendingNotes(ctx, window, limit, offset, mode, store.TrustQualificationPolicy{
		MaxHops:      policy.MaxHops,
		MinimumScore: policy.MinimumScore,
	}, maxStaleness)
	if err != nil {
		return nil, false, err
	}
	out := make([]trustedNoteCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustedNoteCandidate{
			note:    trendingNoteFromStore(row.Note),
			trusted: row.Trusted,
		})
	}
	return out, ready, nil
}

type legacyTrendingProfilesCapability interface {
	GetTrendingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]store.TrendingProfile, error)
}

type legacyRelatedProfilesCapability interface {
	GetRelatedProfiles(ctx context.Context, pubkey string, limit int) ([]store.RelatedProfile, error)
}

type legacyTrustQualifiedTrendingProfilesCapability interface {
	GetTrustQualifiedTrendingProfiles(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		rising bool,
		mode string,
		policy store.TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]store.TrustQualifiedTrendingProfile, bool, error)
}

type legacyTrendingProfilesAdapter struct {
	legacy legacyTrendingProfilesCapability
}

type legacyRelatedProfilesAdapter struct {
	legacy legacyRelatedProfilesCapability
}

func (a legacyTrendingProfilesAdapter) GetTrendingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]TrendingProfile, error) {
	rows, err := a.legacy.GetTrendingProfiles(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingProfile, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingProfileFromStore(row))
	}
	return out, nil
}

func (a legacyRelatedProfilesAdapter) GetRelatedProfiles(
	ctx context.Context,
	pubkey string,
	limit int,
) ([]RelatedProfile, error) {
	rows, err := a.legacy.GetRelatedProfiles(ctx, pubkey, limit)
	if err != nil {
		return nil, err
	}
	out := make([]RelatedProfile, 0, len(rows))
	for _, row := range rows {
		out = append(out, relatedProfileFromStore(row))
	}
	return out, nil
}

type legacyTrustQualifiedTrendingProfilesAdapter struct {
	legacy legacyTrustQualifiedTrendingProfilesCapability
}

func (a legacyTrustQualifiedTrendingProfilesAdapter) GetTrustQualifiedTrendingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	rising bool,
	mode string,
	policy TrustQualificationPolicy,
	maxStaleness time.Duration,
) ([]trustedProfileCandidate, bool, error) {
	rows, ready, err := a.legacy.GetTrustQualifiedTrendingProfiles(ctx, window, limit, offset, rising, mode, store.TrustQualificationPolicy{
		MaxHops:      policy.MaxHops,
		MinimumScore: policy.MinimumScore,
	}, maxStaleness)
	if err != nil {
		return nil, false, err
	}
	out := make([]trustedProfileCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustedProfileCandidate{
			profile: trendingProfileFromStore(row.Profile),
			trusted: row.Trusted,
		})
	}
	return out, ready, nil
}

type legacyRisingProfilesCapability interface {
	GetRisingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]store.TrendingProfile, error)
}

type legacyGroupedNoteAnalyticsCapability interface {
	GetGroupedNoteAnalytics(ctx context.Context, req store.GroupedNoteAnalyticsQuery) (store.GroupedNoteAnalyticsProjection, error)
}

type legacyRisingProfilesAdapter struct {
	legacy legacyRisingProfilesCapability
}

type legacyGroupedNoteAnalyticsAdapter struct {
	legacy legacyGroupedNoteAnalyticsCapability
}

func (a legacyRisingProfilesAdapter) GetRisingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]TrendingProfile, error) {
	rows, err := a.legacy.GetRisingProfiles(ctx, window, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]TrendingProfile, 0, len(rows))
	for _, row := range rows {
		out = append(out, trendingProfileFromStore(row))
	}
	return out, nil
}

func (a legacyGroupedNoteAnalyticsAdapter) GetGroupedNoteAnalytics(
	ctx context.Context,
	req GroupedNoteAnalyticsRequest,
) (GroupedNoteAnalyticsSummary, error) {
	row, err := a.legacy.GetGroupedNoteAnalytics(ctx, store.GroupedNoteAnalyticsQuery{
		Pubkey:        req.Pubkey,
		WindowDays:    req.WindowDays,
		GroupKind:     req.GroupKind,
		GroupKey:      req.GroupKey,
		MetadataTag:   req.MetadataTag,
		TopNotesLimit: req.TopNotesLimit,
		TopicsLimit:   req.TopicsLimit,
	})
	if err != nil {
		return GroupedNoteAnalyticsSummary{}, err
	}
	return groupedNoteAnalyticsFromStore(row), nil
}
