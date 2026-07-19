package query

// Whole-group capability wiring used by the typed production path
// (NewServiceFromStore). Each function takes a typed capability group and
// populates the internal query-shaped capability slots through the existing
// per-capability mapper adapters. Unlike the reader-assertion path used for
// partial test fakes, these wire every slot unconditionally: the group
// interface guarantees at compile time that all methods are present.

func wireCuratedGroup(g CuratedReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.curated.networkStats = legacyNetworkStatsAdapter{legacy: g}
	caps.curated.publicNetworkStats = legacyPublicDiscoveryNetworkStatsAdapter{legacy: g}
	caps.curated.values = g
	caps.curated.recommendedReads = legacyCuratedRecommendedReadsAdapter{legacy: g}
	caps.curated.readsTopics = legacyCuratedReadsTopicsAdapter{legacy: g}
	caps.curated.trendingHashtags = legacyTrendingHashtagsAdapter{legacy: g}
	caps.curated.hashtagSummary = legacyHashtagSummaryAdapter{legacy: g}
	caps.curated.hashtagNotes = legacyHashtagNotesAdapter{legacy: g}
	caps.curated.relatedHashtags = legacyRelatedHashtagsAdapter{legacy: g}
	caps.curated.eventLinkedDomains = legacyEventLinkedDomainsAdapter{legacy: g}
	caps.curated.topDomains = legacyTopDomainsAdapter{legacy: g}
	caps.curated.topDomainsByAuthor = legacyTopDomainsByAuthorAdapter{legacy: g}
	caps.curated.trendingDomains = legacyTrendingDomainsAdapter{legacy: g}
	caps.curated.domainSummary = legacyDomainSummaryAdapter{legacy: g}
	caps.curated.domainNotes = legacyDomainNotesAdapter{legacy: g}
	caps.curated.trendingNotes = legacyTrendingNotesAdapter{legacy: g}
	caps.curated.trendingLongForm = legacyTrendingLongFormAdapter{legacy: g}
	caps.curated.hotConversations = legacyHotConversationsAdapter{legacy: g}
	caps.curated.trustQualifiedNotes = legacyTrustQualifiedTrendingNotesAdapter{legacy: g}
	caps.curated.trendingProfiles = legacyTrendingProfilesAdapter{legacy: g}
	caps.curated.relatedProfiles = legacyRelatedProfilesAdapter{legacy: g}
	caps.curated.trustQualifiedProfiles = legacyTrustQualifiedTrendingProfilesAdapter{legacy: g}
	caps.curated.risingProfiles = legacyRisingProfilesAdapter{legacy: g}
	caps.curated.featuredAuthors = legacyCuratedFeaturedAuthorsAdapter{legacy: g}
	caps.curated.creatorPaidTiers = g
	caps.curated.pubkeyByLNAddress = g
	caps.curated.groupedNoteAnalytics = legacyGroupedNoteAnalyticsAdapter{legacy: g}
}

func wireTrustGroup(g TrustReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	trust := legacyTrustAdapter{legacy: g}
	caps.trust.state = trust
	caps.trust.score = trust
	caps.trust.topPubkeys = trust
	caps.trust.run = trust
	caps.trust.runs = trust
	caps.trust.qualification = legacyTrustQualificationAdapter{legacy: g}
}

func wireDMGroup(g DMReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.dm.directMessages = g
	caps.dm.contacts = g
	caps.dm.contactsDetailed = g
	caps.dm.withRange = g
	caps.dm.unreadCounts = g
	caps.dm.unreadReset = g
	caps.dm.count = g
	caps.dm.directMessageCountOps = g
}

func wireModerationGroup(g ModerationReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.moderation.listByKind = g
	caps.moderation.listByIdentifier = g
	caps.moderation.hiddenByContent = g
	caps.moderation.mutedBy = g
}

func wireReplaceableGroup(g ReplaceableReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.replaceable.event = g
	caps.replaceable.list = g
	caps.replaceable.listByIdentifier = g
	caps.replaceable.events = g
	caps.replaceable.longFormATagReplies = g
}

func wireSocialGroup(g SocialReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.social.userFollowing = g
	caps.social.mutualFollows = g
}

func wireEventGroup(g EventReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.event.userZaps = g
	caps.event.highlightsByEventID = g
	caps.event.highlightsByATarget = g
	caps.event.eventZapsBySats = g
	caps.event.authorSentZaps = g
	caps.event.authorReactions = g
	caps.event.authorRecentEventsByKind = g
}

func wireThreadGroup(g ThreadReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.thread.summary = legacyThreadSummaryAdapter{legacy: g}
}

func wireNotePageGroup(g NotePageReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.notePage.noteStats = legacyNoteStatsAdapter{legacy: g}
	caps.notePage.conversationVelocity = legacyNoteConversationVelocityAdapter{legacy: g}
	caps.notePage.relatedNotes = legacyRelatedNotesAdapter{legacy: g}
	caps.notePage.quoteRepostLinkage = legacyNoteQuoteRepostLinkageAdapter{legacy: g}
}
