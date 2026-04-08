package query

type serviceCapabilities struct {
	dm          dmCapabilities
	moderation  moderationCapabilities
	curated     curatedCapabilities
	trust       trustCapabilities
	replaceable replaceableCapabilities
	social      socialCapabilities
	event       eventCapabilities
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
}

type curatedCapabilities struct {
	networkStats           networkStatsCapability
	publicNetworkStats     publicDiscoveryNetworkStatsCapability
	values                 curatedValuesCapability
	recommendedReads       curatedRecommendedReadsCapability
	readsTopics            curatedReadsTopicsCapability
	trendingNotes          trendingNotesCapability
	trustQualifiedNotes    trustQualifiedTrendingNotesCapability
	trendingHashtags       trendingHashtagsCapability
	trendingProfiles       trendingProfilesCapability
	trustQualifiedProfiles trustQualifiedTrendingProfilesCapability
	risingProfiles         risingProfilesCapability
	featuredAuthors        curatedFeaturedAuthorsCapability
	creatorPaidTiers       creatorPaidTiersCapability
	pubkeyByLNAddress      pubkeyByLNAddressCapability
}

type trustCapabilities struct {
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
	userZaps            userZapsCapability
	highlightsByEventID highlightsByEventIDCapability
	highlightsByATarget highlightsByATargetCapability
	eventZapsBySats     eventZapsBySatsCapability
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
	return caps
}
