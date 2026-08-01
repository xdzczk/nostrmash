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
	statsSeries            discoveryStatsSeriesCapability
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
	homeTrendingDomains    homeTrendingDomainsCapability
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

// adaptServiceCapabilities builds the internal capability slots. Each group is
// populated either from an explicit typed group on ServiceOptions (the typed
// production path, wired whole) or, when no group is supplied, by asserting the
// readmodel-shaped capability interfaces on the reader (the partial-fake path used
// by tests). Neither path uses an untyped `any` value.
func adaptServiceCapabilities(reader any, options ServiceOptions) serviceCapabilities {
	caps := serviceCapabilities{}
	if options.DM != nil {
		wireDMGroup(options.DM, &caps)
	} else {
		adaptDMCapabilities(reader, &caps)
	}
	if options.Moderation != nil {
		wireModerationGroup(options.Moderation, &caps)
	} else {
		adaptModerationCapabilities(reader, &caps)
	}
	if options.Curated != nil {
		wireCuratedGroup(options.Curated, &caps)
	} else {
		adaptCuratedCapabilities(reader, &caps)
	}
	if options.Trust != nil {
		wireTrustGroup(options.Trust, &caps)
	} else {
		adaptTrustCapabilities(reader, &caps)
	}
	if options.Replaceable != nil {
		wireReplaceableGroup(options.Replaceable, &caps)
	} else {
		adaptReplaceableCapabilities(reader, &caps)
	}
	if options.Social != nil {
		wireSocialGroup(options.Social, &caps)
	} else {
		adaptSocialCapabilities(reader, &caps)
	}
	if options.Event != nil {
		wireEventGroup(options.Event, &caps)
	} else {
		adaptEventCapabilities(reader, &caps)
	}
	if options.Thread != nil {
		wireThreadGroup(options.Thread, &caps)
	} else {
		adaptThreadCapabilities(reader, &caps)
	}
	if options.NotePage != nil {
		wireNotePageGroup(options.NotePage, &caps)
	} else {
		adaptNotePageCapabilities(reader, &caps)
	}
	return caps
}
