package query

import (
	"context"
	"encoding/json"
	"time"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// Curated capability interfaces are readmodel-shaped: they describe exactly what
// the production store returns. The Service maps these readmodel projections to
// query DTOs at the response edge via the mappers_*.go helpers. There is a
// single interface family (no native/readmodel twins) and no wrapper adapters.

type networkStatsCapability interface {
	GetNetworkStats(ctx context.Context) (readmodel.NetworkStats, error)
}

type publicDiscoveryNetworkStatsCapability interface {
	GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (readmodel.PublicDiscoveryNetworkStats, error)
}

type discoveryStatsSeriesCapability interface {
	GetDiscoveryStatsSeries(ctx context.Context, metric string, window time.Duration) (readmodel.DiscoveryStatsSeries, error)
}

type curatedValuesCapability interface {
	GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error)
}

type curatedRecommendedReadsCapability interface {
	GetCuratedRecommendedReads(ctx context.Context, limit int) ([]readmodel.CuratedRecommendedRead, error)
}

type curatedReadsTopicsCapability interface {
	GetCuratedReadsTopics(ctx context.Context, limit int) ([]readmodel.CuratedReadsTopic, error)
}

type trendingHashtagsCapability interface {
	GetTrendingHashtags(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.TrendingHashtag, error)
}

type hashtagSummaryCapability interface {
	GetHashtagSummary(ctx context.Context, hashtag string) (readmodel.HashtagSummary, error)
}

type hashtagNotesCapability interface {
	GetHashtagNotes(ctx context.Context, hashtag string, sort string, window string, limit int, offset int) ([]readmodel.TrendingNote, error)
}

type relatedHashtagsCapability interface {
	GetRelatedHashtags(ctx context.Context, hashtag string, limit int) ([]readmodel.RelatedHashtag, error)
}

type eventLinkedDomainsCapability interface {
	GetEventLinkedDomains(ctx context.Context, eventID string, limit int) ([]readmodel.EventDomainLinkProjection, error)
}

type topDomainsCapability interface {
	GetTopDomains(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.DomainStatProjection, error)
}

type topDomainsByAuthorCapability interface {
	GetTopDomainsByAuthor(ctx context.Context, pubkey string, window time.Duration, limit int, offset int) ([]readmodel.DomainStatProjection, error)
}

type trendingDomainsCapability interface {
	GetTrendingDomains(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.DomainSummaryProjection, error)
}

// homeTrendingDomainsCapability serves the homepage's fixed (24h/7d, top-N)
// trending-domains shape from a precomputed snapshot instead of running the
// live COUNT(DISTINCT) aggregate behind trendingDomainsCapability on every
// request. See internal/derivation/projection_relay_window_snapshots.go and
// internal/store/read/parity_domains.go for the snapshot writer/reader.
type homeTrendingDomainsCapability interface {
	GetHomeTrendingDomains(ctx context.Context, window time.Duration, limit int) ([]readmodel.DomainSummaryProjection, error)
}

type domainSummaryCapability interface {
	GetDomainSummary(ctx context.Context, domain string, recentLimit int, topLimit int) (readmodel.DomainSummaryProjection, error)
}

type domainNotesCapability interface {
	GetDomainNotes(ctx context.Context, domain string, sort string, window string, limit int, offset int) ([]readmodel.TrendingNote, error)
}

type trendingNotesCapability interface {
	GetTrendingNotes(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.TrendingNote, error)
}

type trendingLongFormCapability interface {
	GetTrendingLongForm(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.TrendingNote, error)
}

type hotConversationsCapability interface {
	GetHotConversations(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.HotConversation, error)
}

type trustQualifiedTrendingNotesCapability interface {
	GetTrustQualifiedTrendingNotes(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		mode string,
		policy readmodel.TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]readmodel.TrustQualifiedTrendingNote, bool, error)
}

type trendingProfilesCapability interface {
	GetTrendingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.TrendingProfile, error)
}

type relatedProfilesCapability interface {
	GetRelatedProfiles(ctx context.Context, pubkey string, limit int) ([]readmodel.RelatedProfile, error)
}

type trustQualifiedTrendingProfilesCapability interface {
	GetTrustQualifiedTrendingProfiles(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		rising bool,
		mode string,
		policy readmodel.TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]readmodel.TrustQualifiedTrendingProfile, bool, error)
}

type risingProfilesCapability interface {
	GetRisingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.TrendingProfile, error)
}

type curatedFeaturedAuthorsCapability interface {
	GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]readmodel.CuratedFeaturedAuthor, error)
}

type creatorPaidTiersCapability interface {
	GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error)
}

type pubkeyByLNAddressCapability interface {
	GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error)
}

type groupedNoteAnalyticsCapability interface {
	GetGroupedNoteAnalytics(ctx context.Context, req readmodel.GroupedNoteAnalyticsQuery) (readmodel.GroupedNoteAnalyticsProjection, error)
}
