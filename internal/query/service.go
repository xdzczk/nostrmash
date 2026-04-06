package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
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

// ThreadReader is the minimal dependency needed for thread assembly orchestration.
type ThreadReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventReplies(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
}

// EventReader is the minimal dependency needed for event lookup/count orchestration.
type EventReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error)
}

// ProfileReader is the minimal dependency needed for profile/user-info orchestration.
type ProfileReader interface {
	GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error)
	GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error)
}

type Service struct {
	reader Reader
}

func NewService(reader Reader) Service {
	return Service{reader: reader}
}

type threadService struct {
	reader ThreadReader
}

type eventService struct {
	reader EventReader
}

type profileService struct {
	reader ProfileReader
}

// NewThreadService constructs a thread-only orchestration service from a narrow dependency.
func NewThreadService(reader ThreadReader) ThreadService {
	return threadService{reader: reader}
}

// NewEventService constructs an event-only orchestration service from a narrow dependency.
func NewEventService(reader EventReader) EventService {
	return eventService{reader: reader}
}

// NewProfileService constructs a profile-only orchestration service from a narrow dependency.
func NewProfileService(reader ProfileReader) ProfileService {
	return profileService{reader: reader}
}

var (
	_ ThreadService     = Service{}
	_ EventService      = Service{}
	_ ProfileService    = Service{}
	_ ReadOrchestration = Service{}

	// ErrThreadEventNotFound indicates the focal/root event for a thread was not found.
	// This is intentionally narrower than store.ErrNotFound so transports can preserve
	// historical status code behavior for non-root thread fetch failures.
	ErrThreadEventNotFound = errors.New("thread event not found")
)

func (s Service) GetThread(ctx context.Context, req ThreadRequest) (out ThreadView, err error) {
	return threadService{reader: s.reader}.GetThread(ctx, req)
}

func (s threadService) GetThread(ctx context.Context, req ThreadRequest) (out ThreadView, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_thread")
	defer func() { span.End(err) }()
	out = ThreadView{Consistency: "eventual"}
	eventID := strings.TrimSpace(req.EventID)
	if eventID == "" {
		return out, fmt.Errorf("event id is required")
	}
	raw, err := s.reader.GetEventRawByID(ctx, eventID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return out, ErrThreadEventNotFound
		}
		return out, err
	}
	ancestors, missing, err := s.reader.GetEventAncestors(ctx, eventID, req.MaxDepth)
	if err != nil {
		return out, err
	}
	replies, next, err := s.reader.GetEventReplies(ctx, eventID, req.Limit, req.Cursor)
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

func (s Service) GetThreadWindow(ctx context.Context, req ThreadWindowRequest) (out ThreadView, err error) {
	return threadService{reader: s.reader}.GetThreadWindow(ctx, req)
}

func (s threadService) GetThreadWindow(ctx context.Context, req ThreadWindowRequest) (out ThreadView, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_thread_window")
	defer func() { span.End(err) }()
	const fetchPageSize = 100
	eventID := strings.TrimSpace(req.EventID)
	if eventID == "" {
		return ThreadView{}, fmt.Errorf("event id is required")
	}
	raw, err := s.reader.GetEventRawByID(ctx, eventID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ThreadView{}, ErrThreadEventNotFound
		}
		return ThreadView{}, err
	}
	ancestors, missing, err := s.reader.GetEventAncestors(ctx, eventID, req.MaxDepth)
	if err != nil {
		return ThreadView{}, err
	}
	var ascCursor *store.EventOrderCursor
	collected := make([]json.RawMessage, 0, fetchPageSize)
	type cursorKey struct {
		createdAt int64
		id        string
	}
	seenCursors := map[cursorKey]struct{}{}
	for {
		replies, nextCursor, err := s.reader.GetEventReplies(ctx, eventID, fetchPageSize, ascCursor)
		if err != nil {
			return ThreadView{}, err
		}
		collected = append(collected, replies...)
		if nextCursor == nil || len(replies) == 0 {
			break
		}
		key := cursorKey{
			createdAt: nextCursor.CreatedAt,
			id:        strings.TrimSpace(nextCursor.ID),
		}
		if _, seen := seenCursors[key]; seen {
			break
		}
		seenCursors[key] = struct{}{}
		ascCursor = nextCursor
	}

	out = ThreadView{
		Event:              raw,
		Ancestors:          ancestors,
		MissingAncestorIDs: missing,
		Consistency:        "eventual",
	}
	window, next := WindowDescendingReplies(collected, nil, req.Limit, req.Cursor, req.Offset)
	out.Replies = window
	out.NextCursor = next
	return out, nil
}

type orderedReply struct {
	raw       json.RawMessage
	createdAt int64
	id        string
}

func toDescendingReplies(values []json.RawMessage) []orderedReply {
	out := make([]orderedReply, 0, len(values))
	for i := len(values) - 1; i >= 0; i-- {
		var payload struct {
			ID        string `json:"id"`
			CreatedAt int64  `json:"created_at"`
		}
		if err := json.Unmarshal(values[i], &payload); err != nil {
			continue
		}
		payload.ID = strings.TrimSpace(payload.ID)
		if payload.ID == "" {
			continue
		}
		out = append(out, orderedReply{
			raw:       values[i],
			createdAt: payload.CreatedAt,
			id:        payload.ID,
		})
	}
	return out
}

func paginateReplies(
	descReplies []orderedReply,
	limit int,
	cursor *store.EventOrderCursor,
	offset int,
) ([]json.RawMessage, *store.EventOrderCursor) {
	start := offset
	if cursor != nil {
		start = len(descReplies)
		for idx, reply := range descReplies {
			if reply.id == cursor.ID && reply.createdAt == cursor.CreatedAt {
				start = idx + 1
				break
			}
		}
	}
	if start < 0 {
		start = 0
	}
	if start > len(descReplies) {
		start = len(descReplies)
	}
	end := start + limit
	if end > len(descReplies) {
		end = len(descReplies)
	}
	window := descReplies[start:end]
	out := make([]json.RawMessage, 0, len(window))
	for _, reply := range window {
		out = append(out, reply.raw)
	}
	var next *store.EventOrderCursor
	if end < len(descReplies) && len(window) > 0 {
		last := window[len(window)-1]
		next = &store.EventOrderCursor{
			CreatedAt: last.createdAt,
			ID:        last.id,
		}
	}
	return out, next
}

func toOrderedReplies(values []json.RawMessage) []orderedReply {
	out := make([]orderedReply, 0, len(values))
	for _, value := range values {
		var payload struct {
			ID        string `json:"id"`
			CreatedAt int64  `json:"created_at"`
		}
		if err := json.Unmarshal(value, &payload); err != nil {
			continue
		}
		payload.ID = strings.TrimSpace(payload.ID)
		if payload.ID == "" {
			continue
		}
		out = append(out, orderedReply{
			raw:       value,
			createdAt: payload.CreatedAt,
			id:        payload.ID,
		})
	}
	return out
}

func mergeOrderedReplies(base []orderedReply, extra []orderedReply) []orderedReply {
	seen := make(map[string]struct{}, len(base)+len(extra))
	merged := make([]orderedReply, 0, len(base)+len(extra))
	appendUnique := func(values []orderedReply) {
		for _, value := range values {
			if value.id == "" {
				continue
			}
			if _, ok := seen[value.id]; ok {
				continue
			}
			seen[value.id] = struct{}{}
			merged = append(merged, value)
		}
	}
	appendUnique(base)
	appendUnique(extra)
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].createdAt == merged[j].createdAt {
			return merged[i].id > merged[j].id
		}
		return merged[i].createdAt > merged[j].createdAt
	})
	return merged
}

// WindowDescendingReplies merges descending reply collections and applies cursor/offset paging.
func WindowDescendingReplies(
	baseReplies []json.RawMessage,
	extraReplies []json.RawMessage,
	limit int,
	cursor *store.EventOrderCursor,
	offset int,
) ([]json.RawMessage, *store.EventOrderCursor) {
	baseOrdered := toDescendingReplies(baseReplies)
	if len(extraReplies) == 0 {
		return paginateReplies(baseOrdered, limit, cursor, offset)
	}
	extraOrdered := toOrderedReplies(extraReplies)
	merged := mergeOrderedReplies(baseOrdered, extraOrdered)
	return paginateReplies(merged, limit, cursor, offset)
}

func (s Service) GetThreadView(
	ctx context.Context,
	eventID string,
	limit int,
	maxDepth int,
	cursor *store.EventOrderCursor,
) (out ThreadView, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_thread_view")
	defer func() { span.End(err) }()
	out = ThreadView{Consistency: "eventual"}
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

func (s Service) GetActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	return eventService{reader: s.reader}.GetEventActionCounts(ctx, eventID)
}

func (s eventService) GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
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

func (s Service) GetEvent(ctx context.Context, id string) (json.RawMessage, error) {
	return s.GetEventByID(ctx, id)
}

func (s eventService) GetEvent(ctx context.Context, id string) (json.RawMessage, error) {
	return s.reader.GetEventRawByID(ctx, id)
}

func (s Service) GetEvents(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return s.GetEventBatch(ctx, ids)
}

func (s eventService) GetEvents(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return s.reader.GetEventRawsByIDs(ctx, ids)
}

func (s Service) GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	return s.GetActionCounts(ctx, eventID)
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

func (s Service) GetProfiles(ctx context.Context, pubkeys []string) (UserInfosResult, error) {
	return s.GetUserInfos(ctx, pubkeys)
}

func (s profileService) GetProfile(ctx context.Context, pubkey string) (store.ProfileProjection, error) {
	return s.reader.GetProfileByPubkey(ctx, pubkey)
}

func (s profileService) GetProfiles(ctx context.Context, pubkeys []string) (UserInfosResult, error) {
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

func (s Service) Search(ctx context.Context, text string, limit int) (out SearchResult, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search")
	defer func() { span.End(err) }()
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

func (s Service) GetEventByID(ctx context.Context, id string) (raw json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_by_id")
	defer func() { span.End(err) }()
	return s.reader.GetEventRawByID(ctx, id)
}

func (s Service) GetEventBatch(ctx context.Context, ids []string) (out map[string]json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_batch")
	defer func() { span.End(err) }()
	return s.reader.GetEventRawsByIDs(ctx, ids)
}

func (s Service) GetProfile(ctx context.Context, pubkey string) (out store.ProfileProjection, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_profile")
	defer func() { span.End(err) }()
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
