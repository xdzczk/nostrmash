package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// EventActionsPage is one page of kind 6, 7, or 9735 events that target a note.
type EventActionsPage struct {
	Events     []json.RawMessage
	NextCursor *EventCursor
}

// MuteListDetail is a kind 10000 list with tag names kept on each entry.
// Event is the raw replaceable event when the index still has it.
type MuteListDetail struct {
	Tags  []MuteListTag
	Event json.RawMessage
}

// MuteListTag is one muted pubkey, event, hashtag, or word.
type MuteListTag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// FollowerCursor resumes a follower-edge page.
type FollowerCursor struct {
	ContactListCreatedAt int64
	SourceEventID        string
	FollowerPubkey       string
}

// NotificationCursor resumes an engagement notification page.
type NotificationCursor struct {
	CreatedAt   int64
	Type        string
	ActorPubkey string
	EventID     string
}

// EngagementNotification is one like, repost, quote, zap, reply, mention, or follow.
type EngagementNotification struct {
	Type        string `json:"type"`
	CreatedAt   int64  `json:"created_at"`
	ActorPubkey string `json:"actor_pubkey"`
	EventID     string `json:"event_id"`
}

// IsUnsupportedEventActionKind reports a kind outside 6, 7, and 9735.
func IsUnsupportedEventActionKind(err error) bool {
	return errors.Is(err, readmodel.ErrUnsupportedEventActionKind)
}

func (s Service) GetEventActionsPage(
	ctx context.Context,
	eventID string,
	kind int,
	limit int,
	cursor *EventCursor,
) (EventActionsPage, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EventActionsPage{}, fmt.Errorf("event id is required")
	}
	if kind != 6 && kind != 7 && kind != 9735 {
		return EventActionsPage{}, readmodel.ErrUnsupportedEventActionKind
	}
	limit = normalizeAuthorEventsLimit(limit)
	reader := s.capabilities.event.actionsPage
	if reader == nil {
		return EventActionsPage{}, unsupportedCapabilityError("event actions")
	}
	events, next, err := reader.ListEventActions(ctx, eventID, kind, limit, eventCursorToStore(cursor))
	if err != nil {
		return EventActionsPage{}, err
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	return EventActionsPage{Events: events, NextCursor: eventCursorFromStore(next)}, nil
}

func (s Service) GetMuteListDetail(ctx context.Context, pubkey string) (MuteListDetail, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return MuteListDetail{}, fmt.Errorf("pubkey is required")
	}
	reader := s.capabilities.moderation.listTags
	if reader == nil {
		return MuteListDetail{}, unsupportedCapabilityError("mute list tags")
	}
	tags, event, err := reader.GetModerationListTags(ctx, pubkey, 10000)
	if err != nil {
		if errors.Is(err, readmodel.ErrNotFound) {
			return MuteListDetail{Tags: []MuteListTag{}}, nil
		}
		return MuteListDetail{}, err
	}
	out := make([]MuteListTag, 0, len(tags))
	for _, tag := range tags {
		name := strings.TrimSpace(tag.Name)
		value := strings.TrimSpace(tag.Value)
		if name == "" || value == "" {
			continue
		}
		out = append(out, MuteListTag{Name: name, Value: value})
	}
	return MuteListDetail{Tags: out, Event: event}, nil
}

func (s Service) GetFollowersPage(
	ctx context.Context,
	pubkey string,
	limit int,
	cursor *FollowerCursor,
) ([]json.RawMessage, *FollowerCursor, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, nil, fmt.Errorf("pubkey is required")
	}
	limit = normalizeAuthorEventsLimit(limit)
	reader := s.capabilities.social.followersPage
	if reader == nil {
		if cursor != nil {
			return nil, nil, unsupportedCapabilityError("follower pagination")
		}
		items, err := s.GetFollowers(ctx, pubkey, limit)
		return items, nil, err
	}
	items, next, err := reader.GetFollowersByPubkeyPage(ctx, pubkey, limit, followerCursorToStore(cursor))
	if err != nil {
		return nil, nil, err
	}
	if items == nil {
		items = []json.RawMessage{}
	}
	return items, followerCursorFromStore(next), nil
}

// GetMutualFollowsPage lists mutual pubkeys. The same pubkey for both
// arguments is that account's mutuals. afterPubkey is the last pubkey of the
// previous page; empty starts at the beginning. The result includes at most
// limit pubkeys. next is the last pubkey when another page exists.
func (s Service) GetMutualFollowsPage(
	ctx context.Context,
	pubkey string,
	userPubkey string,
	limit int,
	afterPubkey string,
) (pubkeys []string, next string, err error) {
	pubkey = strings.TrimSpace(pubkey)
	userPubkey = strings.TrimSpace(userPubkey)
	afterPubkey = strings.TrimSpace(afterPubkey)
	if pubkey == "" || userPubkey == "" {
		return nil, "", fmt.Errorf("pubkey is required")
	}
	limit = normalizeAuthorEventsLimit(limit)
	reader := s.capabilities.social.mutualFollowsPage
	if reader == nil {
		return nil, "", unsupportedCapabilityError("mutual follows")
	}
	// Ask for one extra row so the caller can tell a full page from the last page.
	rows, err := reader.GetMutualFollowsPage(ctx, pubkey, userPubkey, limit, afterPubkey)
	if err != nil {
		return nil, "", err
	}
	if rows == nil {
		rows = []string{}
	}
	if len(rows) > limit {
		rows = rows[:limit]
		if len(rows) > 0 {
			next = rows[len(rows)-1]
		}
	}
	return rows, next, nil
}

func (s Service) ListEngagementNotifications(
	ctx context.Context,
	pubkey string,
	limit int,
	cursor *NotificationCursor,
) ([]EngagementNotification, *NotificationCursor, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, nil, fmt.Errorf("pubkey is required")
	}
	limit = normalizeAuthorEventsLimit(limit)
	reader := s.capabilities.event.notificationsPage
	if reader == nil {
		return nil, nil, unsupportedCapabilityError("notifications")
	}
	rows, next, err := reader.ListEngagementNotifications(ctx, pubkey, limit, notificationCursorToStore(cursor))
	if err != nil {
		return nil, nil, err
	}
	out := make([]EngagementNotification, 0, len(rows))
	for _, row := range rows {
		out = append(out, EngagementNotification{
			Type:        row.Type,
			CreatedAt:   row.CreatedAt,
			ActorPubkey: row.ActorPubkey,
			EventID:     row.EventID,
		})
	}
	return out, notificationCursorFromStore(next), nil
}

func (s Service) GetAuthorRepliesPage(
	ctx context.Context,
	pubkey string,
	limit int,
	cursor *EventCursor,
) (AuthorEventsResult, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorEventsResult{}, fmt.Errorf("pubkey is required")
	}
	limit = normalizeAuthorEventsLimit(limit)
	reader := s.capabilities.event.authorRepliesPage
	if reader == nil {
		if cursor != nil {
			return AuthorEventsResult{}, unsupportedCapabilityError("author reply pagination")
		}
		events, err := s.GetAuthorReplies(ctx, pubkey, limit)
		if err != nil {
			return AuthorEventsResult{}, err
		}
		return AuthorEventsResult{Pubkey: pubkey, Events: events, Consistency: "eventual"}, nil
	}
	events, next, err := reader.GetAuthorRepliesPage(ctx, pubkey, limit, eventCursorToStore(cursor))
	if err != nil {
		return AuthorEventsResult{}, err
	}
	if cursor == nil {
		events = s.maybeAugmentAuthorEventsFromRelays(ctx, pubkey, []int{1}, limit, events)
		events = keepAuthorReplyEvents(events)
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	return AuthorEventsResult{
		Pubkey:      pubkey,
		Events:      events,
		NextCursor:  eventCursorFromStore(next),
		Consistency: "eventual",
	}, nil
}

func (s Service) GetLongFormPage(
	ctx context.Context,
	pubkey string,
	limit int,
	cursor *EventCursor,
) (AuthorEventsResult, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorEventsResult{}, fmt.Errorf("pubkey is required")
	}
	limit = normalizeAuthorEventsLimit(limit)
	reader := s.capabilities.event.recentEventsByKindPage
	if reader == nil {
		if cursor != nil {
			return AuthorEventsResult{}, unsupportedCapabilityError("long-form pagination")
		}
		events, err := s.GetRecentEventsByKindAndPubkey(ctx, 30023, pubkey, limit)
		if err != nil {
			return AuthorEventsResult{}, err
		}
		return AuthorEventsResult{Pubkey: pubkey, Events: events, Consistency: "eventual"}, nil
	}
	events, next, err := reader.GetRecentEventsByKindAndPubkeyPage(ctx, 30023, pubkey, limit, eventCursorToStore(cursor))
	if err != nil {
		return AuthorEventsResult{}, err
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	return AuthorEventsResult{
		Pubkey:      pubkey,
		Events:      events,
		NextCursor:  eventCursorFromStore(next),
		Consistency: "eventual",
	}, nil
}

func followerCursorToStore(cursor *FollowerCursor) *readmodel.FollowerCursor {
	if cursor == nil {
		return nil
	}
	return &readmodel.FollowerCursor{
		ContactListCreatedAt: cursor.ContactListCreatedAt,
		SourceEventID:        cursor.SourceEventID,
		FollowerPubkey:       cursor.FollowerPubkey,
	}
}

func followerCursorFromStore(cursor *readmodel.FollowerCursor) *FollowerCursor {
	if cursor == nil {
		return nil
	}
	return &FollowerCursor{
		ContactListCreatedAt: cursor.ContactListCreatedAt,
		SourceEventID:        cursor.SourceEventID,
		FollowerPubkey:       cursor.FollowerPubkey,
	}
}

func notificationCursorToStore(cursor *NotificationCursor) *readmodel.NotificationCursor {
	if cursor == nil {
		return nil
	}
	return &readmodel.NotificationCursor{
		CreatedAt:   cursor.CreatedAt,
		Type:        cursor.Type,
		ActorPubkey: cursor.ActorPubkey,
		EventID:     cursor.EventID,
	}
}

func notificationCursorFromStore(cursor *readmodel.NotificationCursor) *NotificationCursor {
	if cursor == nil {
		return nil
	}
	return &NotificationCursor{
		CreatedAt:   cursor.CreatedAt,
		Type:        cursor.Type,
		ActorPubkey: cursor.ActorPubkey,
		EventID:     cursor.EventID,
	}
}
