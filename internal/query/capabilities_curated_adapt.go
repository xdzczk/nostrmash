package query

func adaptCuratedCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(networkStatsCapability); ok {
		caps.curated.networkStats = r
	} else if legacy, ok := reader.(legacyNetworkStatsCapability); ok {
		caps.curated.networkStats = legacyNetworkStatsAdapter{legacy: legacy}
	}
	if r, ok := reader.(publicDiscoveryNetworkStatsCapability); ok {
		caps.curated.publicNetworkStats = r
	} else if legacy, ok := reader.(legacyPublicDiscoveryNetworkStatsCapability); ok {
		caps.curated.publicNetworkStats = legacyPublicDiscoveryNetworkStatsAdapter{legacy: legacy}
	}
	if r, ok := reader.(curatedValuesCapability); ok {
		caps.curated.values = r
	}
	if r, ok := reader.(curatedRecommendedReadsCapability); ok {
		caps.curated.recommendedReads = r
	} else if legacy, ok := reader.(legacyCuratedRecommendedReadsCapability); ok {
		caps.curated.recommendedReads = legacyCuratedRecommendedReadsAdapter{legacy: legacy}
	}
	if r, ok := reader.(curatedReadsTopicsCapability); ok {
		caps.curated.readsTopics = r
	} else if legacy, ok := reader.(legacyCuratedReadsTopicsCapability); ok {
		caps.curated.readsTopics = legacyCuratedReadsTopicsAdapter{legacy: legacy}
	}
	if r, ok := reader.(trendingHashtagsCapability); ok {
		caps.curated.trendingHashtags = r
	} else if legacy, ok := reader.(legacyTrendingHashtagsCapability); ok {
		caps.curated.trendingHashtags = legacyTrendingHashtagsAdapter{legacy: legacy}
	}
	if r, ok := reader.(hashtagSummaryCapability); ok {
		caps.curated.hashtagSummary = r
	} else if legacy, ok := reader.(legacyHashtagSummaryCapability); ok {
		caps.curated.hashtagSummary = legacyHashtagSummaryAdapter{legacy: legacy}
	}
	if r, ok := reader.(hashtagNotesCapability); ok {
		caps.curated.hashtagNotes = r
	} else if legacy, ok := reader.(legacyHashtagNotesCapability); ok {
		caps.curated.hashtagNotes = legacyHashtagNotesAdapter{legacy: legacy}
	}
	if r, ok := reader.(relatedHashtagsCapability); ok {
		caps.curated.relatedHashtags = r
	} else if legacy, ok := reader.(legacyRelatedHashtagsCapability); ok {
		caps.curated.relatedHashtags = legacyRelatedHashtagsAdapter{legacy: legacy}
	}
	if r, ok := reader.(eventLinkedDomainsCapability); ok {
		caps.curated.eventLinkedDomains = r
	} else if legacy, ok := reader.(legacyEventLinkedDomainsCapability); ok {
		caps.curated.eventLinkedDomains = legacyEventLinkedDomainsAdapter{legacy: legacy}
	}
	if r, ok := reader.(topDomainsCapability); ok {
		caps.curated.topDomains = r
	} else if legacy, ok := reader.(legacyTopDomainsCapability); ok {
		caps.curated.topDomains = legacyTopDomainsAdapter{legacy: legacy}
	}
	if r, ok := reader.(topDomainsByAuthorCapability); ok {
		caps.curated.topDomainsByAuthor = r
	} else if legacy, ok := reader.(legacyTopDomainsByAuthorCapability); ok {
		caps.curated.topDomainsByAuthor = legacyTopDomainsByAuthorAdapter{legacy: legacy}
	}
	if r, ok := reader.(trendingDomainsCapability); ok {
		caps.curated.trendingDomains = r
	} else if legacy, ok := reader.(legacyTrendingDomainsCapability); ok {
		caps.curated.trendingDomains = legacyTrendingDomainsAdapter{legacy: legacy}
	}
	if r, ok := reader.(domainSummaryCapability); ok {
		caps.curated.domainSummary = r
	} else if legacy, ok := reader.(legacyDomainSummaryCapability); ok {
		caps.curated.domainSummary = legacyDomainSummaryAdapter{legacy: legacy}
	}
	if r, ok := reader.(domainNotesCapability); ok {
		caps.curated.domainNotes = r
	} else if legacy, ok := reader.(legacyDomainNotesCapability); ok {
		caps.curated.domainNotes = legacyDomainNotesAdapter{legacy: legacy}
	}
	if r, ok := reader.(trendingNotesCapability); ok {
		caps.curated.trendingNotes = r
	} else if legacy, ok := reader.(legacyTrendingNotesCapability); ok {
		caps.curated.trendingNotes = legacyTrendingNotesAdapter{legacy: legacy}
	}
	if r, ok := reader.(trendingLongFormCapability); ok {
		caps.curated.trendingLongForm = r
	} else if legacy, ok := reader.(legacyTrendingLongFormCapability); ok {
		caps.curated.trendingLongForm = legacyTrendingLongFormAdapter{legacy: legacy}
	}
	if r, ok := reader.(hotConversationsCapability); ok {
		caps.curated.hotConversations = r
	} else if legacy, ok := reader.(legacyHotConversationsCapability); ok {
		caps.curated.hotConversations = legacyHotConversationsAdapter{legacy: legacy}
	}
	if r, ok := reader.(trustQualifiedTrendingNotesCapability); ok {
		caps.curated.trustQualifiedNotes = r
	} else if legacy, ok := reader.(legacyTrustQualifiedTrendingNotesCapability); ok {
		caps.curated.trustQualifiedNotes = legacyTrustQualifiedTrendingNotesAdapter{legacy: legacy}
	}
	if r, ok := reader.(trendingProfilesCapability); ok {
		caps.curated.trendingProfiles = r
	} else if legacy, ok := reader.(legacyTrendingProfilesCapability); ok {
		caps.curated.trendingProfiles = legacyTrendingProfilesAdapter{legacy: legacy}
	}
	if r, ok := reader.(relatedProfilesCapability); ok {
		caps.curated.relatedProfiles = r
	} else if legacy, ok := reader.(legacyRelatedProfilesCapability); ok {
		caps.curated.relatedProfiles = legacyRelatedProfilesAdapter{legacy: legacy}
	}
	if r, ok := reader.(trustQualifiedTrendingProfilesCapability); ok {
		caps.curated.trustQualifiedProfiles = r
	} else if legacy, ok := reader.(legacyTrustQualifiedTrendingProfilesCapability); ok {
		caps.curated.trustQualifiedProfiles = legacyTrustQualifiedTrendingProfilesAdapter{legacy: legacy}
	}
	if r, ok := reader.(risingProfilesCapability); ok {
		caps.curated.risingProfiles = r
	} else if legacy, ok := reader.(legacyRisingProfilesCapability); ok {
		caps.curated.risingProfiles = legacyRisingProfilesAdapter{legacy: legacy}
	}
	if r, ok := reader.(curatedFeaturedAuthorsCapability); ok {
		caps.curated.featuredAuthors = r
	} else if legacy, ok := reader.(legacyCuratedFeaturedAuthorsCapability); ok {
		caps.curated.featuredAuthors = legacyCuratedFeaturedAuthorsAdapter{legacy: legacy}
	}
	if r, ok := reader.(creatorPaidTiersCapability); ok {
		caps.curated.creatorPaidTiers = r
	}
	if r, ok := reader.(pubkeyByLNAddressCapability); ok {
		caps.curated.pubkeyByLNAddress = r
	}
	if r, ok := reader.(groupedNoteAnalyticsCapability); ok {
		caps.curated.groupedNoteAnalytics = r
	} else if legacy, ok := reader.(legacyGroupedNoteAnalyticsCapability); ok {
		caps.curated.groupedNoteAnalytics = legacyGroupedNoteAnalyticsAdapter{legacy: legacy}
	}
}
