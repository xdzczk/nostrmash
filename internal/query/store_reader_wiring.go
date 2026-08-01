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
	caps.curated.networkStats = g
	caps.curated.publicNetworkStats = g
	caps.curated.statsSeries = g
	caps.curated.values = g
	caps.curated.recommendedReads = g
	caps.curated.readsTopics = g
	caps.curated.trendingHashtags = g
	caps.curated.hashtagSummary = g
	caps.curated.hashtagNotes = g
	caps.curated.relatedHashtags = g
	caps.curated.eventLinkedDomains = g
	caps.curated.topDomains = g
	caps.curated.topDomainsByAuthor = g
	caps.curated.trendingDomains = g
	caps.curated.homeTrendingDomains = g
	caps.curated.domainSummary = g
	caps.curated.domainNotes = g
	caps.curated.trendingNotes = g
	caps.curated.trendingLongForm = g
	caps.curated.hotConversations = g
	caps.curated.trustQualifiedNotes = g
	caps.curated.trendingProfiles = g
	caps.curated.relatedProfiles = g
	caps.curated.trustQualifiedProfiles = g
	caps.curated.risingProfiles = g
	caps.curated.featuredAuthors = g
	caps.curated.creatorPaidTiers = g
	caps.curated.pubkeyByLNAddress = g
	caps.curated.groupedNoteAnalytics = g
}

func wireTrustGroup(g TrustReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.trust.state = g
	caps.trust.score = g
	caps.trust.topPubkeys = g
	caps.trust.run = g
	caps.trust.runs = g
	caps.trust.qualification = g
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
	caps.thread.summary = g
}

func wireNotePageGroup(g NotePageReads, caps *serviceCapabilities) {
	if g == nil {
		return
	}
	caps.notePage.noteStats = g
	caps.notePage.conversationVelocity = g
	caps.notePage.relatedNotes = g
	caps.notePage.quoteRepostLinkage = g
}
