package readmodel

import "encoding/json"

// ProfileFollowerCount is a pubkey's projected follower total.
type ProfileFollowerCount struct {
	Pubkey        string
	FollowerCount int64
}

// ZapTotals is the received-zap aggregate for one pubkey.
type ZapTotals struct {
	Count int64
	Sats  int64
}

// EventRelayHint is one relay known to have carried an event.
type EventRelayHint struct {
	EventID  string
	RelayURL string
}

// ViewerEventAction records whether the viewer engaged with an event.
type ViewerEventAction struct {
	EventID  string
	Liked    bool
	Replied  bool
	Reposted bool
	Zapped   bool
}

// EventZap is one zap receipt attached to a target event.
type EventZap struct {
	EventID      string
	CreatedAt    int64
	Sender       string
	Receiver     string
	AmountSats   int64
	ZapReceiptID string
}

// PrimalNotification is one stored notification row.
type PrimalNotification struct {
	Pubkey    string
	Type      int
	CreatedAt int64
	Args      json.RawMessage
}

// NotificationCount is one unread type counter.
type NotificationCount struct {
	Type int
	Cnt  int64
}

// MediaMetadata is a stored media resource description.
type MediaMetadata struct {
	URL      string
	Mimetype string
	Width    int
	Height   int
	Duration int
	Blurhash string
}

// LinkMetadata is a stored URL preview.
type LinkMetadata struct {
	URL         string
	Title       string
	Description string
	Image       string
	IconURL     string
}
