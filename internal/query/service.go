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
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 10003, pubkey, limit)
}

func (s Service) GetHighlights(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9802, pubkey, limit)
}

func (s Service) GetLongForm(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 30023, pubkey, limit)
}

func (s Service) GetZaps(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	// Nostr kind 9735 commonly represents zap receipts.
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9735, pubkey, limit)
}

func (s Service) GetDirectMessages(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	// Nostr kind 4 represents encrypted direct messages.
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 4, pubkey, limit)
}

func (s Service) GetFollowers(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	// Approximate follower discovery using mention/reference projections.
	return s.reader.GetEventsReferencingPubkey(ctx, pubkey, limit)
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
