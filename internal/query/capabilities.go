package query

import (
	"context"
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/store"
)

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
	networkStats      networkStatsCapability
	values            curatedValuesCapability
	recommendedReads  curatedRecommendedReadsCapability
	readsTopics       curatedReadsTopicsCapability
	featuredAuthors   curatedFeaturedAuthorsCapability
	creatorPaidTiers  creatorPaidTiersCapability
	pubkeyByLNAddress pubkeyByLNAddressCapability
}

type trustCapabilities struct {
	score      trustScoreCapability
	topPubkeys topTrustedPubkeysCapability
	run        trustRunCapability
	runs       trustRunsCapability
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

type directMessagesCapability interface {
	GetDirectMessages(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error)
}

type dmContactsCapability interface {
	GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error)
}

type dmContactsDetailedCapability interface {
	GetDirectMessageContactsDetailed(ctx context.Context, receiver string, limit int, offset int, since int64, until int64) ([]json.RawMessage, error)
}

type directMessagesWithRangeCapability interface {
	GetDirectMessagesWithRange(ctx context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error)
}

type dmUnreadCountsCapability interface {
	GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
}

type dmUnreadResetCapability interface {
	ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error
}

type dmCountCapability interface {
	GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error)
}

type dmCountResetCapability interface {
	ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error
	ResetDirectMessageCounts(ctx context.Context, receiver string) error
}

type moderationListByKindCapability interface {
	GetModerationList(ctx context.Context, pubkey string, kind int) ([]string, error)
}

type moderationListByIdentifierCapability interface {
	GetModerationListByIdentifier(ctx context.Context, pubkey string, identifier string) ([]string, error)
}

type hiddenByContentModerationCapability interface {
	IsHiddenByContentModeration(ctx context.Context, viewerPubkey string, eventID string) (bool, string, error)
}

type networkStatsCapability interface {
	GetNetworkStats(ctx context.Context) (NetworkStats, error)
}

type curatedValuesCapability interface {
	GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error)
}

type curatedRecommendedReadsCapability interface {
	GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error)
}

type curatedReadsTopicsCapability interface {
	GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error)
}

type curatedFeaturedAuthorsCapability interface {
	GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error)
}

type creatorPaidTiersCapability interface {
	GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error)
}

type pubkeyByLNAddressCapability interface {
	GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error)
}

type trustScoreCapability interface {
	GetTrustScore(ctx context.Context, pubkey string) (TrustScore, error)
}

type topTrustedPubkeysCapability interface {
	ListTopTrustedPubkeys(ctx context.Context, limit int) ([]TrustScore, error)
}

type trustRunCapability interface {
	GetTrustRun(ctx context.Context, runID int64) (TrustRun, error)
}

type trustRunsCapability interface {
	ListTrustRuns(ctx context.Context, limit int) ([]TrustRun, error)
}

type parameterizedReplaceableEventCapability interface {
	GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error)
}

type parameterizedReplaceableListCapability interface {
	GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error)
}

type parameterizedReplaceableListByIdentifierCapability interface {
	GetParameterizedReplaceableListByIdentifier(ctx context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error)
}

type parameterizedReplaceableEventsCapability interface {
	GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error)
}

type eventsByATagAndKindCapability interface {
	GetEventsByATagAndKind(ctx context.Context, kind int, aTagValue string, limit int) ([]json.RawMessage, error)
}

type isUserFollowingCapability interface {
	IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error)
}

type mutualFollowsCapability interface {
	GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error)
}

type userZapsCapability interface {
	GetUserZaps(ctx context.Context, pubkey string, limit int, sortBySats bool) ([]json.RawMessage, error)
}

type highlightsByEventIDCapability interface {
	GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error)
}

type highlightsByATargetCapability interface {
	GetHighlightsByATarget(ctx context.Context, kind int, pubkey string, identifier string, limit int) ([]json.RawMessage, error)
}

type eventZapsBySatsCapability interface {
	GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error)
}

func adaptServiceCapabilities(reader any) serviceCapabilities {
	caps := serviceCapabilities{}

	if r, ok := reader.(directMessagesCapability); ok {
		caps.dm.directMessages = r
	}
	if r, ok := reader.(dmContactsCapability); ok {
		caps.dm.contacts = r
	}
	if r, ok := reader.(dmContactsDetailedCapability); ok {
		caps.dm.contactsDetailed = r
	}
	if r, ok := reader.(directMessagesWithRangeCapability); ok {
		caps.dm.withRange = r
	}
	if r, ok := reader.(dmUnreadCountsCapability); ok {
		caps.dm.unreadCounts = r
	}
	if r, ok := reader.(dmUnreadResetCapability); ok {
		caps.dm.unreadReset = r
	}
	if r, ok := reader.(dmCountCapability); ok {
		caps.dm.count = r
	}
	if r, ok := reader.(dmCountResetCapability); ok {
		caps.dm.directMessageCountOps = r
	}

	if r, ok := reader.(moderationListByKindCapability); ok {
		caps.moderation.listByKind = r
	}
	if r, ok := reader.(moderationListByIdentifierCapability); ok {
		caps.moderation.listByIdentifier = r
	}
	if r, ok := reader.(hiddenByContentModerationCapability); ok {
		caps.moderation.hiddenByContent = r
	}

	if r, ok := reader.(networkStatsCapability); ok {
		caps.curated.networkStats = r
	} else if legacy, ok := reader.(legacyNetworkStatsCapability); ok {
		caps.curated.networkStats = legacyNetworkStatsAdapter{legacy: legacy}
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

	if r, ok := reader.(trustScoreCapability); ok {
		caps.trust.score = r
	}
	if r, ok := reader.(topTrustedPubkeysCapability); ok {
		caps.trust.topPubkeys = r
	}
	if r, ok := reader.(trustRunCapability); ok {
		caps.trust.run = r
	}
	if r, ok := reader.(trustRunsCapability); ok {
		caps.trust.runs = r
	}
	if legacy, ok := reader.(legacyTrustCapability); ok {
		adapted := legacyTrustAdapter{legacy: legacy}
		if caps.trust.score == nil {
			caps.trust.score = adapted
		}
		if caps.trust.topPubkeys == nil {
			caps.trust.topPubkeys = adapted
		}
		if caps.trust.run == nil {
			caps.trust.run = adapted
		}
		if caps.trust.runs == nil {
			caps.trust.runs = adapted
		}
	}

	if r, ok := reader.(parameterizedReplaceableEventCapability); ok {
		caps.replaceable.event = r
	}
	if r, ok := reader.(parameterizedReplaceableListCapability); ok {
		caps.replaceable.list = r
	}
	if r, ok := reader.(parameterizedReplaceableListByIdentifierCapability); ok {
		caps.replaceable.listByIdentifier = r
	}
	if r, ok := reader.(parameterizedReplaceableEventsCapability); ok {
		caps.replaceable.events = r
	}
	if r, ok := reader.(eventsByATagAndKindCapability); ok {
		caps.replaceable.longFormATagReplies = r
	}

	if r, ok := reader.(isUserFollowingCapability); ok {
		caps.social.userFollowing = r
	}
	if r, ok := reader.(mutualFollowsCapability); ok {
		caps.social.mutualFollows = r
	}

	if r, ok := reader.(userZapsCapability); ok {
		caps.event.userZaps = r
	}
	if r, ok := reader.(highlightsByEventIDCapability); ok {
		caps.event.highlightsByEventID = r
	}
	if r, ok := reader.(highlightsByATargetCapability); ok {
		caps.event.highlightsByATarget = r
	}
	if r, ok := reader.(eventZapsBySatsCapability); ok {
		caps.event.eventZapsBySats = r
	}

	return caps
}

type legacyNetworkStatsCapability interface {
	GetNetworkStats(ctx context.Context) (store.NetworkStats, error)
}

type legacyNetworkStatsAdapter struct {
	legacy legacyNetworkStatsCapability
}

func (a legacyNetworkStatsAdapter) GetNetworkStats(ctx context.Context) (NetworkStats, error) {
	row, err := a.legacy.GetNetworkStats(ctx)
	if err != nil {
		return NetworkStats{}, err
	}
	return networkStatsFromStore(row), nil
}

type legacyCuratedRecommendedReadsCapability interface {
	GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error)
}

type legacyCuratedRecommendedReadsAdapter struct {
	legacy legacyCuratedRecommendedReadsCapability
}

func (a legacyCuratedRecommendedReadsAdapter) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error) {
	rows, err := a.legacy.GetCuratedRecommendedReads(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CuratedRecommendedRead, 0, len(rows))
	for _, row := range rows {
		out = append(out, curatedRecommendedReadFromStore(row))
	}
	return out, nil
}

type legacyCuratedReadsTopicsCapability interface {
	GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error)
}

type legacyCuratedReadsTopicsAdapter struct {
	legacy legacyCuratedReadsTopicsCapability
}

func (a legacyCuratedReadsTopicsAdapter) GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error) {
	rows, err := a.legacy.GetCuratedReadsTopics(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CuratedReadsTopic, 0, len(rows))
	for _, row := range rows {
		out = append(out, curatedReadsTopicFromStore(row))
	}
	return out, nil
}

type legacyCuratedFeaturedAuthorsCapability interface {
	GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error)
}

type legacyCuratedFeaturedAuthorsAdapter struct {
	legacy legacyCuratedFeaturedAuthorsCapability
}

func (a legacyCuratedFeaturedAuthorsAdapter) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error) {
	rows, err := a.legacy.GetCuratedFeaturedAuthors(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CuratedFeaturedAuthor, 0, len(rows))
	for _, row := range rows {
		out = append(out, curatedFeaturedAuthorFromStore(row))
	}
	return out, nil
}

type legacyTrustCapability interface {
	GetTrustScore(ctx context.Context, pubkey string) (store.TrustGlobalScore, error)
	ListTopTrustedPubkeys(ctx context.Context, limit int) ([]store.TrustGlobalScore, error)
	GetTrustRun(ctx context.Context, runID int64) (store.TrustRun, error)
	ListTrustRuns(ctx context.Context, limit int) ([]store.TrustRun, error)
}

type legacyTrustAdapter struct {
	legacy legacyTrustCapability
}

func (a legacyTrustAdapter) GetTrustScore(ctx context.Context, pubkey string) (TrustScore, error) {
	row, err := a.legacy.GetTrustScore(ctx, pubkey)
	if err != nil {
		return TrustScore{}, err
	}
	return trustScoreFromStore(row), nil
}

func (a legacyTrustAdapter) ListTopTrustedPubkeys(ctx context.Context, limit int) ([]TrustScore, error) {
	rows, err := a.legacy.ListTopTrustedPubkeys(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]TrustScore, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustScoreFromStore(row))
	}
	return out, nil
}

func (a legacyTrustAdapter) GetTrustRun(ctx context.Context, runID int64) (TrustRun, error) {
	row, err := a.legacy.GetTrustRun(ctx, runID)
	if err != nil {
		return TrustRun{}, err
	}
	return trustRunFromStore(row), nil
}

func (a legacyTrustAdapter) ListTrustRuns(ctx context.Context, limit int) ([]TrustRun, error) {
	rows, err := a.legacy.ListTrustRuns(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]TrustRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustRunFromStore(row))
	}
	return out, nil
}
