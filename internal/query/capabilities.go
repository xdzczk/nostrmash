package query

type serviceCapabilities struct {
	dm          dmCapabilities
	moderation  moderationCapabilities
	curated     curatedCapabilities
	trust       trustCapabilities
	replaceable replaceableCapabilities
	social      socialCapabilities
	event       eventCapabilities
	thread      threadCapabilities
	notePage    notePageCapabilities
}

type dmCapabilities struct {
	directMessages        directMessagesCapability
	contacts              dmContactsCapability
	contactsDetailed      dmContactsDetailedCapability
	withRange             directMessagesWithRangeCapability
	unreadCounts          dmUnreadCountsCapability
	unreadReset           dmUnreadResetCapability
	count                 dmCountCapability
	directMessageCountOps dmCountResetCapability
}

type moderationCapabilities struct {
	listByKind       moderationListByKindCapability
	listByIdentifier moderationListByIdentifierCapability
	hiddenByContent  hiddenByContentModerationCapability
	mutedBy          moderationMutedByCapability
}

type curatedCapabilities struct {
	networkStats           networkStatsCapability
	publicNetworkStats     publicDiscoveryNetworkStatsCapability
	values                 curatedValuesCapability
	recommendedReads       curatedRecommendedReadsCapability
	readsTopics            curatedReadsTopicsCapability
	trendingNotes          trendingNotesCapability
	trendingLongForm       trendingLongFormCapability
	hotConversations       hotConversationsCapability
	trustQualifiedNotes    trustQualifiedTrendingNotesCapability
	trendingHashtags       trendingHashtagsCapability
	hashtagSummary         hashtagSummaryCapability
	hashtagNotes           hashtagNotesCapability
	relatedHashtags        relatedHashtagsCapability
	eventLinkedDomains     eventLinkedDomainsCapability
	topDomains             topDomainsCapability
	topDomainsByAuthor     topDomainsByAuthorCapability
	trendingDomains        trendingDomainsCapability
	domainSummary          domainSummaryCapability
	domainNotes            domainNotesCapability
	trendingProfiles       trendingProfilesCapability
	relatedProfiles        relatedProfilesCapability
	trustQualifiedProfiles trustQualifiedTrendingProfilesCapability
	risingProfiles         risingProfilesCapability
	featuredAuthors        curatedFeaturedAuthorsCapability
	creatorPaidTiers       creatorPaidTiersCapability
	pubkeyByLNAddress      pubkeyByLNAddressCapability
	groupedNoteAnalytics   groupedNoteAnalyticsCapability
}

type trustCapabilities struct {
	state         trustStateCapability
	score         trustScoreCapability
	topPubkeys    topTrustedPubkeysCapability
	run           trustRunCapability
	runs          trustRunsCapability
	qualification trustQualificationCapability
}

type replaceableCapabilities struct {
	event               parameterizedReplaceableEventCapability
	list                parameterizedReplaceableListCapability
	listByIdentifier    parameterizedReplaceableListByIdentifierCapability
	events              parameterizedReplaceableEventsCapability
	longFormATagReplies eventsByATagAndKindCapability
}

type socialCapabilities struct {
	userFollowing isUserFollowingCapability
	mutualFollows mutualFollowsCapability
}

type eventCapabilities struct {
	userZaps                 userZapsCapability
	highlightsByEventID      highlightsByEventIDCapability
	highlightsByATarget      highlightsByATargetCapability
	eventZapsBySats          eventZapsBySatsCapability
	authorSentZaps           authorSentZapsCapability
	authorReactions          authorReactionsCapability
	authorRecentEventsByKind authorRecentEventsByKindCapability
}

type threadCapabilities struct {
	summary threadSummaryCapability
}

type notePageCapabilities struct {
	noteStats            noteStatsCapability
	conversationVelocity noteConversationVelocityCapability
	relatedNotes         relatedNotesCapability
	quoteRepostLinkage   noteQuoteRepostLinkageCapability
}

func adaptServiceCapabilities(reader any) serviceCapabilities {
	caps := serviceCapabilities{}
	adaptDMCapabilities(reader, &caps)
	adaptModerationCapabilities(reader, &caps)
	adaptCuratedCapabilities(reader, &caps)
	adaptTrustCapabilities(reader, &caps)
	adaptReplaceableCapabilities(reader, &caps)
	adaptSocialCapabilities(reader, &caps)
	adaptEventCapabilities(reader, &caps)
	adaptThreadCapabilities(reader, &caps)
	adaptNotePageCapabilities(reader, &caps)
	return caps
}
