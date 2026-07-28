package query

// adaptCuratedCapabilities populates the curated slots by asserting the
// readmodel-shaped capability interfaces on a partial reader (the test-fake
// path). Missing capabilities stay nil, preserving ErrUnsupportedCapability.
func adaptCuratedCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(networkStatsCapability); ok {
		caps.curated.networkStats = r
	}
	if r, ok := reader.(publicDiscoveryNetworkStatsCapability); ok {
		caps.curated.publicNetworkStats = r
	}
	if r, ok := reader.(discoveryStatsSeriesCapability); ok {
		caps.curated.statsSeries = r
	}
	if r, ok := reader.(curatedValuesCapability); ok {
		caps.curated.values = r
	}
	if r, ok := reader.(curatedRecommendedReadsCapability); ok {
		caps.curated.recommendedReads = r
	}
	if r, ok := reader.(curatedReadsTopicsCapability); ok {
		caps.curated.readsTopics = r
	}
	if r, ok := reader.(trendingHashtagsCapability); ok {
		caps.curated.trendingHashtags = r
	}
	if r, ok := reader.(hashtagSummaryCapability); ok {
		caps.curated.hashtagSummary = r
	}
	if r, ok := reader.(hashtagNotesCapability); ok {
		caps.curated.hashtagNotes = r
	}
	if r, ok := reader.(relatedHashtagsCapability); ok {
		caps.curated.relatedHashtags = r
	}
	if r, ok := reader.(eventLinkedDomainsCapability); ok {
		caps.curated.eventLinkedDomains = r
	}
	if r, ok := reader.(topDomainsCapability); ok {
		caps.curated.topDomains = r
	}
	if r, ok := reader.(topDomainsByAuthorCapability); ok {
		caps.curated.topDomainsByAuthor = r
	}
	if r, ok := reader.(trendingDomainsCapability); ok {
		caps.curated.trendingDomains = r
	}
	if r, ok := reader.(domainSummaryCapability); ok {
		caps.curated.domainSummary = r
	}
	if r, ok := reader.(domainNotesCapability); ok {
		caps.curated.domainNotes = r
	}
	if r, ok := reader.(trendingNotesCapability); ok {
		caps.curated.trendingNotes = r
	}
	if r, ok := reader.(trendingLongFormCapability); ok {
		caps.curated.trendingLongForm = r
	}
	if r, ok := reader.(hotConversationsCapability); ok {
		caps.curated.hotConversations = r
	}
	if r, ok := reader.(trustQualifiedTrendingNotesCapability); ok {
		caps.curated.trustQualifiedNotes = r
	}
	if r, ok := reader.(trendingProfilesCapability); ok {
		caps.curated.trendingProfiles = r
	}
	if r, ok := reader.(relatedProfilesCapability); ok {
		caps.curated.relatedProfiles = r
	}
	if r, ok := reader.(trustQualifiedTrendingProfilesCapability); ok {
		caps.curated.trustQualifiedProfiles = r
	}
	if r, ok := reader.(risingProfilesCapability); ok {
		caps.curated.risingProfiles = r
	}
	if r, ok := reader.(curatedFeaturedAuthorsCapability); ok {
		caps.curated.featuredAuthors = r
	}
	if r, ok := reader.(creatorPaidTiersCapability); ok {
		caps.curated.creatorPaidTiers = r
	}
	if r, ok := reader.(pubkeyByLNAddressCapability); ok {
		caps.curated.pubkeyByLNAddress = r
	}
	if r, ok := reader.(groupedNoteAnalyticsCapability); ok {
		caps.curated.groupedNoteAnalytics = r
	}
}
