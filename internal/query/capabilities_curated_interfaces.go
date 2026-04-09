package query

import (
	"context"
	"encoding/json"
	"time"
)

type networkStatsCapability interface {
	GetNetworkStats(ctx context.Context) (NetworkStats, error)
}

type publicDiscoveryNetworkStatsCapability interface {
	GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (PublicDiscoveryNetworkStats, error)
}

type curatedValuesCapability interface {
	GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error)
}

type curatedRecommendedReadsCapability interface {
	GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error)
}

type curatedReadsTopicsCapability interface {
	GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error)
}

type trendingHashtagsCapability interface {
	GetTrendingHashtags(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingHashtag, error)
}

type hashtagSummaryCapability interface {
	GetHashtagSummary(ctx context.Context, hashtag string) (HashtagSummary, error)
}

type hashtagNotesCapability interface {
	GetHashtagNotes(ctx context.Context, hashtag string, sort string, window string, limit int, offset int) ([]TrendingNote, error)
}

type relatedHashtagsCapability interface {
	GetRelatedHashtags(ctx context.Context, hashtag string, limit int) ([]RelatedHashtag, error)
}

type eventLinkedDomainsCapability interface {
	GetEventLinkedDomains(ctx context.Context, eventID string, limit int) ([]EventDomainLink, error)
}

type topDomainsCapability interface {
	GetTopDomains(ctx context.Context, window time.Duration, limit int, offset int) ([]DomainStat, error)
}

type topDomainsByAuthorCapability interface {
	GetTopDomainsByAuthor(ctx context.Context, pubkey string, window time.Duration, limit int, offset int) ([]DomainStat, error)
}

type trendingDomainsCapability interface {
	GetTrendingDomains(ctx context.Context, window time.Duration, limit int, offset int) ([]DomainSummary, error)
}

type domainSummaryCapability interface {
	GetDomainSummary(ctx context.Context, domain string, recentLimit int, topLimit int) (DomainSummary, error)
}

type domainNotesCapability interface {
	GetDomainNotes(ctx context.Context, domain string, sort string, window string, limit int, offset int) ([]TrendingNote, error)
}

type trendingNotesCapability interface {
	GetTrendingNotes(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error)
}

type hotConversationsCapability interface {
	GetHotConversations(ctx context.Context, window time.Duration, limit int, offset int) ([]HotConversation, error)
}

type trustQualifiedTrendingNotesCapability interface {
	GetTrustQualifiedTrendingNotes(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		mode string,
		policy TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]trustedNoteCandidate, bool, error)
}

type trendingProfilesCapability interface {
	GetTrendingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error)
}

type relatedProfilesCapability interface {
	GetRelatedProfiles(ctx context.Context, pubkey string, limit int) ([]RelatedProfile, error)
}

type trustQualifiedTrendingProfilesCapability interface {
	GetTrustQualifiedTrendingProfiles(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		rising bool,
		mode string,
		policy TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]trustedProfileCandidate, bool, error)
}

type risingProfilesCapability interface {
	GetRisingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error)
}

type curatedFeaturedAuthorsCapability interface {
	GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error)
}

type creatorPaidTiersCapability interface {
	GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error)
}

type pubkeyByLNAddressCapability interface {
	GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error)
}

type groupedNoteAnalyticsCapability interface {
	GetGroupedNoteAnalytics(ctx context.Context, req GroupedNoteAnalyticsRequest) (GroupedNoteAnalyticsSummary, error)
}
