package query

// This file defines the typed read surface that the query Service consumes.
//
// Historically the Service accepted `reader any` and discovered optional
// capabilities by silently type-probing the value at runtime. That made the
// production wiring untyped and let a missing store method silently disable a
// feature. The interfaces below replace that machine with a compile-time
// contract:
//
//   - StoreReader is the readmodel-shaped core read surface every reader must
//     provide.
//   - The *Reads group interfaces bundle the optional capability families.
//   - FullStoreReader is the union that the production *store.PostgresStore
//     satisfies; NewServiceFromStore wires every capability from a single
//     typed value and a compile-time assertion in the composition root proves
//     the store is complete.
//
// The group interfaces intentionally embed the existing readmodel-shaped
// capability interfaces so the internal query-shaped slots keep being fed by
// the per-capability mapper adapters (see the capabilities_* files); no
// behavior changes, only the wiring becomes typed.

// StoreReader is the readmodel-shaped core read surface implemented by the
// production store. The Service wraps it internally to produce query DTOs.
type StoreReader = legacyReader

// CuratedReads bundles the discovery/curation capability surface.
type CuratedReads interface {
	legacyNetworkStatsCapability
	legacyPublicDiscoveryNetworkStatsCapability
	curatedValuesCapability
	legacyCuratedRecommendedReadsCapability
	legacyCuratedReadsTopicsCapability
	legacyTrendingHashtagsCapability
	legacyHashtagSummaryCapability
	legacyHashtagNotesCapability
	legacyRelatedHashtagsCapability
	legacyEventLinkedDomainsCapability
	legacyTopDomainsCapability
	legacyTopDomainsByAuthorCapability
	legacyTrendingDomainsCapability
	legacyDomainSummaryCapability
	legacyDomainNotesCapability
	legacyTrendingNotesCapability
	legacyTrendingLongFormCapability
	legacyHotConversationsCapability
	legacyTrustQualifiedTrendingNotesCapability
	legacyTrendingProfilesCapability
	legacyRelatedProfilesCapability
	legacyTrustQualifiedTrendingProfilesCapability
	legacyRisingProfilesCapability
	legacyCuratedFeaturedAuthorsCapability
	creatorPaidTiersCapability
	pubkeyByLNAddressCapability
	legacyGroupedNoteAnalyticsCapability
}

// TrustReads bundles the trust-state/score/qualification capability surface.
type TrustReads interface {
	legacyTrustCapability
	legacyTrustQualificationCapability
}

// DMReads bundles the direct-message capability surface.
type DMReads interface {
	directMessagesCapability
	dmContactsCapability
	dmContactsDetailedCapability
	directMessagesWithRangeCapability
	dmUnreadCountsCapability
	dmUnreadResetCapability
	dmCountCapability
	dmCountResetCapability
}

// ModerationReads bundles the moderation capability surface.
type ModerationReads interface {
	moderationListByKindCapability
	moderationListByIdentifierCapability
	hiddenByContentModerationCapability
	moderationMutedByCapability
}

// ReplaceableReads bundles the parameterized-replaceable capability surface.
type ReplaceableReads interface {
	parameterizedReplaceableEventCapability
	parameterizedReplaceableListCapability
	parameterizedReplaceableListByIdentifierCapability
	parameterizedReplaceableEventsCapability
	eventsByATagAndKindCapability
}

// SocialReads bundles the follow-graph capability surface.
type SocialReads interface {
	isUserFollowingCapability
	mutualFollowsCapability
}

// EventReads bundles the event-centric zap/highlight/reaction capability surface.
type EventReads interface {
	userZapsCapability
	highlightsByEventIDCapability
	highlightsByATargetCapability
	eventZapsBySatsCapability
	authorSentZapsCapability
	authorReactionsCapability
	authorRecentEventsByKindCapability
}

// ThreadReads bundles the thread-summary capability surface.
type ThreadReads interface {
	legacyThreadSummaryCapability
}

// NotePageReads bundles the note-page enrichment capability surface.
type NotePageReads interface {
	legacyNoteStatsCapability
	legacyNoteConversationVelocityCapability
	legacyRelatedNotesCapability
	legacyNoteQuoteRepostLinkageCapability
}

// AuthorAnalyticsReads bundles the author-analytics capability surface that the
// core reader adapter previously discovered via secondary type-probes.
type AuthorAnalyticsReads interface {
	legacyAuthorAnalyticsSummaryReader
	legacyAuthorQuoteRepostRecentActivityReader
	legacyAuthorTopicStatsReader
	legacyAuthorTopLanguagesReader
	legacyAuthorRelayFootprintReader
	legacyAuthorMediaMixStatsReader
	legacyAuthorActivityWindowsReader
	legacyAuthorPostingPatternsReader
	legacyAuthorTopNotesReader
	legacyAuthorRecycleCandidatesReader
	legacyAuthorPerformanceAggregateReader
}

// AdvancedSearchReads bundles the advanced search/suggestion capability surface
// that the core reader adapter previously discovered via secondary type-probes.
type AdvancedSearchReads interface {
	legacyNotesSearchReader
	legacyProfilesSearchReader
	legacySearchSuggestionsReader
	legacySearchDocumentsReader
	legacyDescendingThreadWindowReader
}

// FullStoreReader is the complete typed read surface satisfied by the
// production *store.PostgresStore. NewServiceFromStore wires every capability
// from one value; the composition root asserts completeness at compile time
// (var _ query.FullStoreReader = (*store.PostgresStore)(nil)).
type FullStoreReader interface {
	StoreReader
	CuratedReads
	TrustReads
	DMReads
	ModerationReads
	ReplaceableReads
	SocialReads
	EventReads
	ThreadReads
	NotePageReads
	AuthorAnalyticsReads
	AdvancedSearchReads
}
