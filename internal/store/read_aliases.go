package store

import storeread "github.com/xdzczk/nostrmash/internal/store/read"

// The read bounded context now lives in internal/store/read. These aliases
// re-export its exported projection types so existing callers that reference
// store.* keep compiling; the read methods are promoted onto PostgresStore via
// the embedded *storeread.Read.
type (
	ContactListProjection         = storeread.ContactListProjection
	CuratedFeaturedAuthor         = storeread.CuratedFeaturedAuthor
	CuratedReadsTopic             = storeread.CuratedReadsTopic
	CuratedRecommendedRead        = storeread.CuratedRecommendedRead
	HashtagActivity               = storeread.HashtagActivity
	HashtagActivityStats          = storeread.HashtagActivityStats
	HashtagSummary                = storeread.HashtagSummary
	LanguageSummary               = storeread.LanguageSummary
	NetworkStats                  = storeread.NetworkStats
	NoteConversationVelocity      = storeread.NoteConversationVelocity
	NoteStats                     = storeread.NoteStats
	PublicDiscoveryNetworkStats   = storeread.PublicDiscoveryNetworkStats
	RelatedHashtag                = storeread.RelatedHashtag
	RelatedNote                   = storeread.RelatedNote
	RelatedProfile                = storeread.RelatedProfile
	RelayListProjection           = storeread.RelayListProjection
	RelaySummaryStats             = storeread.RelaySummaryStats
	RelayUsageSummary             = storeread.RelayUsageSummary
	TrendingHashtag               = storeread.TrendingHashtag
	TrendingHashtagWindows        = storeread.TrendingHashtagWindows
	TrendingNote                  = storeread.TrendingNote
	TrendingProfile               = storeread.TrendingProfile
	TrustQualifiedTrendingNote    = storeread.TrustQualifiedTrendingNote
	TrustQualifiedTrendingProfile = storeread.TrustQualifiedTrendingProfile
	WindowedCount                 = storeread.WindowedCount
)
