package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

type Reader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventWithProvenance(ctx context.Context, id string) (store.EventWithProvenance, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error)
	GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error)
	GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error)
	GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error)
	GetEventReplies(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
	ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error)
	GetContactListByPubkey(ctx context.Context, pubkey string) (store.ContactListProjection, error)
	GetRelayListByPubkey(ctx context.Context, pubkey string) (store.RelayListProjection, error)
	SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error)
	SearchProfiles(ctx context.Context, query string, limit int) ([]store.ProfileProjection, error)
	GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
	GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
}

type Service struct {
	reader Reader
}

func NewService(reader Reader) Service {
	return Service{reader: reader}
}

type ThreadView struct {
	Event              json.RawMessage
	Ancestors          []json.RawMessage
	MissingAncestorIDs []string
	Replies            []json.RawMessage
	NextCursor         *store.EventOrderCursor
	Consistency        string
}

func (s Service) GetThreadView(
	ctx context.Context,
	eventID string,
	limit int,
	maxDepth int,
	cursor *store.EventOrderCursor,
) (ThreadView, error) {
	out := ThreadView{Consistency: "eventual"}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return out, fmt.Errorf("event id is required")
	}
	raw, err := s.reader.GetEventRawByID(ctx, eventID)
	if err != nil {
		return out, err
	}
	ancestors, missing, err := s.reader.GetEventAncestors(ctx, eventID, maxDepth)
	if err != nil {
		return out, err
	}
	replies, next, err := s.reader.GetEventReplies(ctx, eventID, limit, cursor)
	if err != nil {
		return out, err
	}
	out.Event = raw
	out.Ancestors = ancestors
	out.MissingAncestorIDs = missing
	out.Replies = replies
	out.NextCursor = next
	return out, nil
}

type ActionCounts struct {
	EventID       string `json:"event_id"`
	ReplyCount    int64  `json:"reply_count"`
	ReactionCount int64  `json:"reaction_count"`
	RepostCount   int64  `json:"repost_count"`
	Consistency   string `json:"consistency"`
}

func (s Service) GetActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return ActionCounts{}, fmt.Errorf("event id is required")
	}
	counts, err := s.reader.GetEventCounts(ctx, eventID)
	if err != nil {
		return ActionCounts{}, err
	}
	return ActionCounts{
		EventID:       counts.EventID,
		ReplyCount:    counts.ReplyCount,
		ReactionCount: counts.ReactionCount,
		RepostCount:   counts.RepostCount,
		Consistency:   counts.Consistency,
	}, nil
}

type UserInfosResult struct {
	Profiles       []store.ProfileProjection
	MissingPubkeys []string
}

func (s Service) GetUserInfos(ctx context.Context, pubkeys []string) (UserInfosResult, error) {
	normalized := normalizeUniqueStrings(pubkeys)
	if len(normalized) == 0 {
		return UserInfosResult{}, fmt.Errorf("pubkeys must include at least one non-empty value")
	}
	profilesByPubkey, err := s.reader.GetProfilesByPubkeys(ctx, normalized)
	if err != nil {
		return UserInfosResult{}, err
	}
	out := UserInfosResult{
		Profiles:       make([]store.ProfileProjection, 0, len(profilesByPubkey)),
		MissingPubkeys: make([]string, 0),
	}
	for _, pubkey := range normalized {
		profile, ok := profilesByPubkey[pubkey]
		if !ok {
			out.MissingPubkeys = append(out.MissingPubkeys, pubkey)
			continue
		}
		out.Profiles = append(out.Profiles, profile)
	}
	return out, nil
}

type SearchResult struct {
	Events   []json.RawMessage         `json:"events"`
	Profiles []store.ProfileProjection `json:"profiles"`
}

func (s Service) Search(ctx context.Context, text string, limit int) (SearchResult, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return SearchResult{Events: []json.RawMessage{}, Profiles: []store.ProfileProjection{}}, nil
	}
	events, err := s.reader.SearchEventsByContent(ctx, text, limit)
	if err != nil {
		return SearchResult{}, err
	}
	profiles, err := s.reader.SearchProfiles(ctx, text, limit)
	if err != nil {
		return SearchResult{}, err
	}
	return SearchResult{Events: events, Profiles: profiles}, nil
}

func (s Service) GetContactList(ctx context.Context, pubkey string) (store.ContactListProjection, error) {
	return s.reader.GetContactListByPubkey(ctx, pubkey)
}

func (s Service) GetRelayList(ctx context.Context, pubkey string) (store.RelayListProjection, error) {
	return s.reader.GetRelayListByPubkey(ctx, pubkey)
}

func (s Service) GetBookmarks(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type replaceableEventReader interface {
		GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error)
	}
	if r, ok := s.reader.(replaceableEventReader); ok {
		latest, err := r.GetParameterizedReplaceableEvent(ctx, pubkey, 10003, "")
		if err == nil {
			return []json.RawMessage{latest}, nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 10003, pubkey, 1)
}

func (s Service) GetHighlights(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9802, pubkey, limit)
}

func (s Service) GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	type highlightsByEventReader interface {
		GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(highlightsByEventReader); ok {
		return r.GetHighlightsByEventID(ctx, eventID, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetHighlightsByATarget(
	ctx context.Context,
	kind int,
	pubkey string,
	identifier string,
	limit int,
) ([]json.RawMessage, error) {
	type highlightsByATargetReader interface {
		GetHighlightsByATarget(ctx context.Context, kind int, pubkey string, identifier string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(highlightsByATargetReader); ok {
		return r.GetHighlightsByATarget(ctx, kind, pubkey, identifier, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetLongForm(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableList(ctx, pubkey, 30023, limit)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 30023, pubkey, limit)
}

func (s Service) GetZaps(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type receiverZapsReader interface {
		GetUserZaps(ctx context.Context, pubkey string, limit int, sortBySats bool) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(receiverZapsReader); ok {
		return r.GetUserZaps(ctx, pubkey, limit, false)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9735, pubkey, limit)
}

func (s Service) GetDirectMessages(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type directMessagesReader interface {
		GetDirectMessages(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(directMessagesReader); ok {
		return r.GetDirectMessages(ctx, pubkey, "", limit)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 4, pubkey, limit)
}

func (s Service) GetMentions(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetEventsReferencingPubkey(ctx, pubkey, limit)
}

func (s Service) GetFollowers(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetFollowersByPubkey(ctx, pubkey, limit)
}

func (s Service) GetEventByID(ctx context.Context, id string) (json.RawMessage, error) {
	return s.reader.GetEventRawByID(ctx, id)
}

func (s Service) GetEventBatch(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return s.reader.GetEventRawsByIDs(ctx, ids)
}

func (s Service) GetProfile(ctx context.Context, pubkey string) (store.ProfileProjection, error) {
	return s.reader.GetProfileByPubkey(ctx, pubkey)
}

func (s Service) GetAuthorEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetAuthorRecentEvents(ctx, pubkey, limit)
}

func (s Service) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetAuthorReplies(ctx, pubkey, limit)
}

func (s Service) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
}

func (s Service) GetUserZapsBySats(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type receiverZapsReader interface {
		GetUserZaps(ctx context.Context, pubkey string, limit int, sortBySats bool) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(receiverZapsReader); ok {
		return r.GetUserZaps(ctx, pubkey, limit, true)
	}
	return s.GetZaps(ctx, pubkey, limit)
}

func (s Service) GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	type eventZapsReader interface {
		GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(eventZapsReader); ok {
		return r.GetEventZapsBySats(ctx, eventID, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error) {
	type followingReader interface {
		IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error)
	}
	if r, ok := s.reader.(followingReader); ok {
		return r.IsUserFollowing(ctx, followerPubkey, followedPubkey)
	}
	return false, nil
}

func (s Service) GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error) {
	type mutualReader interface {
		GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error)
	}
	if r, ok := s.reader.(mutualReader); ok {
		return r.GetMutualFollows(ctx, leftPubkey, rightPubkey, limit)
	}
	return []string{}, nil
}

func (s Service) GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error) {
	type dmContactsReader interface {
		GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error)
	}
	if r, ok := s.reader.(dmContactsReader); ok {
		return r.GetDirectMessageContacts(ctx, pubkey, limit)
	}
	return []string{}, nil
}

func (s Service) GetDirectMessageContactsDetailed(
	ctx context.Context,
	pubkey string,
	limit int,
	offset int,
	since int64,
	until int64,
) ([]json.RawMessage, error) {
	type dmContactsDetailedReader interface {
		GetDirectMessageContactsDetailed(ctx context.Context, receiver string, limit int, offset int, since int64, until int64) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(dmContactsDetailedReader); ok {
		return r.GetDirectMessageContactsDetailed(ctx, pubkey, limit, offset, since, until)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetDirectMessagesByPeer(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error) {
	type dmReader interface {
		GetDirectMessages(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(dmReader); ok {
		return r.GetDirectMessages(ctx, pubkey, peer, limit)
	}
	return s.GetDirectMessages(ctx, pubkey, limit)
}

func (s Service) GetDirectMessagesWithRange(
	ctx context.Context,
	pubkey string,
	peer string,
	since int64,
	until int64,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	type dmReader interface {
		GetDirectMessagesWithRange(ctx context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(dmReader); ok {
		return r.GetDirectMessagesWithRange(ctx, pubkey, peer, since, until, limit, offset)
	}
	return s.GetDirectMessagesByPeer(ctx, pubkey, peer, limit)
}

func (s Service) GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type dmUnreadReader interface {
		GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(dmUnreadReader); ok {
		return r.GetDirectMessageUnreadCounts(ctx, pubkey, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error {
	type dmResetReader interface {
		ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error
	}
	if r, ok := s.reader.(dmResetReader); ok {
		return r.ResetDirectMessageUnread(ctx, pubkey, peer)
	}
	return nil
}

func (s Service) GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error) {
	type dmCountReader interface {
		GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error)
	}
	if r, ok := s.reader.(dmCountReader); ok {
		return r.GetDirectMessageCount(ctx, receiver, sender)
	}
	return 0, nil
}

func (s Service) ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error {
	type dmResetReader interface {
		ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error
	}
	if r, ok := s.reader.(dmResetReader); ok {
		return r.ResetDirectMessageCount(ctx, receiver, sender)
	}
	return nil
}

func (s Service) ResetDirectMessageCounts(ctx context.Context, receiver string) error {
	type dmResetReader interface {
		ResetDirectMessageCounts(ctx context.Context, receiver string) error
	}
	if r, ok := s.reader.(dmResetReader); ok {
		return r.ResetDirectMessageCounts(ctx, receiver)
	}
	return nil
}

func (s Service) GetMuteList(ctx context.Context, pubkey string) ([]string, error) {
	type listReader interface {
		GetModerationList(ctx context.Context, pubkey string, kind int) ([]string, error)
	}
	if r, ok := s.reader.(listReader); ok {
		values, err := r.GetModerationList(ctx, pubkey, 10000)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return []string{}, nil
}

func (s Service) GetAllowList(ctx context.Context, pubkey string) ([]string, error) {
	type listReader interface {
		GetModerationList(ctx context.Context, pubkey string, kind int) ([]string, error)
	}
	if r, ok := s.reader.(listReader); ok {
		values, err := r.GetModerationList(ctx, pubkey, 10001)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return []string{}, nil
}

func (s Service) GetMuteLists(ctx context.Context, pubkey string) ([]string, error) {
	type listReader interface {
		GetModerationListByIdentifier(ctx context.Context, pubkey string, identifier string) ([]string, error)
	}
	if r, ok := s.reader.(listReader); ok {
		values, err := r.GetModerationListByIdentifier(ctx, pubkey, "mutelists")
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return []string{}, nil
}

func (s Service) GetIdentifierAllowList(ctx context.Context, pubkey string) ([]string, error) {
	type listReader interface {
		GetModerationListByIdentifier(ctx context.Context, pubkey string, identifier string) ([]string, error)
	}
	if r, ok := s.reader.(listReader); ok {
		values, err := r.GetModerationListByIdentifier(ctx, pubkey, "allowlist")
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return []string{}, nil
}

func (s Service) IsHiddenByContentModeration(ctx context.Context, viewerPubkey string, eventID string) (bool, string, error) {
	type moderationReader interface {
		IsHiddenByContentModeration(ctx context.Context, viewerPubkey string, eventID string) (bool, string, error)
	}
	if r, ok := s.reader.(moderationReader); ok {
		return r.IsHiddenByContentModeration(ctx, viewerPubkey, eventID)
	}
	return false, "", nil
}

func (s Service) GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableList(ctx, pubkey, kind, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetParameterizedReplaceableListByIdentifier(ctx context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableListByIdentifier(ctx context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableListByIdentifier(ctx, pubkey, kind, identifier, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error)
	}
	if r, ok := s.reader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableEvent(ctx, pubkey, kind, dTag)
	}
	return nil, store.ErrNotFound
}

func (s Service) GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableEvents(ctx, kind, dTag, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetNetworkStats(ctx context.Context) (store.NetworkStats, error) {
	type statsReader interface {
		GetNetworkStats(ctx context.Context) (store.NetworkStats, error)
	}
	if r, ok := s.reader.(statsReader); ok {
		return r.GetNetworkStats(ctx)
	}
	return store.NetworkStats{}, nil
}

func (s Service) GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error) {
	type curatedReader interface {
		GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCuratedValues(ctx, tableName, valueColumn, limit)
	}
	return []string{}, nil
}

func (s Service) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error) {
	type curatedReader interface {
		GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCuratedRecommendedReads(ctx, limit)
	}
	return []store.CuratedRecommendedRead{}, nil
}

func (s Service) GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error) {
	type curatedReader interface {
		GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCuratedReadsTopics(ctx, limit)
	}
	return []store.CuratedReadsTopic{}, nil
}

func (s Service) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error) {
	type curatedReader interface {
		GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCuratedFeaturedAuthors(ctx, limit)
	}
	return []store.CuratedFeaturedAuthor{}, nil
}

func (s Service) GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error) {
	type curatedReader interface {
		GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetCreatorPaidTiers(ctx, pubkey)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error) {
	type curatedReader interface {
		GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error)
	}
	if r, ok := s.reader.(curatedReader); ok {
		return r.GetPubkeyByLNAddress(ctx, lnAddress)
	}
	return "", store.ErrNotFound
}

func (s Service) GetLongFormThreadView(
	ctx context.Context,
	pubkey string,
	kind int,
	identifier string,
	limit int,
	maxDepth int,
) (ThreadView, error) {
	event, err := s.GetParameterizedReplaceableEvent(ctx, pubkey, kind, identifier)
	if err != nil {
		return ThreadView{}, err
	}
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(event, &payload); err != nil {
		return ThreadView{}, err
	}
	payload.ID = strings.TrimSpace(payload.ID)
	if payload.ID == "" {
		return ThreadView{}, fmt.Errorf("long form event id is missing")
	}
	return s.GetThreadView(ctx, payload.ID, limit, maxDepth, nil)
}

func (s Service) GetLongFormThreadATagReplies(
	ctx context.Context,
	kind int,
	pubkey string,
	identifier string,
	limit int,
) ([]json.RawMessage, error) {
	type longFormATagRepliesReader interface {
		GetEventsByATagAndKind(ctx context.Context, kind int, aTagValue string, limit int) ([]json.RawMessage, error)
	}
	if kind <= 0 {
		return []json.RawMessage{}, nil
	}
	pubkey = strings.TrimSpace(pubkey)
	identifier = strings.TrimSpace(identifier)
	if pubkey == "" || identifier == "" {
		return []json.RawMessage{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 5000 {
		limit = 5000
	}
	if r, ok := s.reader.(longFormATagRepliesReader); ok {
		target := fmt.Sprintf("%d:%s:%s", kind, pubkey, identifier)
		return r.GetEventsByATagAndKind(ctx, 1, target, limit)
	}
	return []json.RawMessage{}, nil
}

func normalizeUniqueStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func IsNotFound(err error) bool {
	return errors.Is(err, store.ErrNotFound)
}
