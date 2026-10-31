package readmodel

import "errors"

// ErrUnsupportedEventActionKind is returned when an event-actions read asks
// for a kind other than reposts (6), reactions (7), or zap receipts (9735).
// An empty page is the wrong signal: clients treat a page that does not
// contain the requested kind as unusable.
var ErrUnsupportedEventActionKind = errors.New("unsupported event action kind")

// ModerationTag is one mute-list entry with its tag name kept intact.
// A flat hex string cannot tell a pubkey (p) from an event id (e).
type ModerationTag struct {
	Name  string
	Value string
}

// FollowerCursor is the keyset position for follower edges ordered by
// contact_list_created_at DESC, source_event_id DESC, follower_pubkey ASC.
// The pubkey is part of the key because one contact list contributes many
// followers that share the same timestamp and source event.
type FollowerCursor struct {
	ContactListCreatedAt int64
	SourceEventID        string
	FollowerPubkey       string
}

// NotificationCursor is the keyset position for an engagement notification
// page ordered by created_at DESC, type DESC, actor DESC, event id DESC.
type NotificationCursor struct {
	CreatedAt   int64
	Type        string
	ActorPubkey string
	EventID     string
}

// EngagementNotification is one like, repost, quote, zap, reply, mention,
// or new follow addressed to a pubkey.
type EngagementNotification struct {
	Type        string
	CreatedAt   int64
	ActorPubkey string
	EventID     string
}
