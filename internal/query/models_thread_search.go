package query

import (
	"encoding/json"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

// ThreadRequest captures transport-agnostic inputs for assembling one thread view.
type ThreadRequest struct {
	EventID  string
	Limit    int
	MaxDepth int
	Cursor   *EventCursor
}

// ThreadWindowRequest captures transport-agnostic inputs for descending-window thread lookups.
type ThreadWindowRequest struct {
	EventID  string
	Limit    int
	MaxDepth int
	Cursor   *EventCursor
	Offset   int
}

type EventCursor struct {
	CreatedAt int64
	ID        string
}

type ThreadView struct {
	Event              json.RawMessage
	Ancestors          []json.RawMessage
	MissingAncestorIDs []string
	Replies            []json.RawMessage
	NextCursor         *EventCursor
	Consistency        string
}

type ThreadVelocityHints struct {
	Replies24h int64
	Replies7d  int64
}

type ThreadSummary struct {
	RootEventID      string
	ReplyCount       int64
	ParticipantCount int
	MaxDepth         int
	LastActivityAt   int64
	Velocity         ThreadVelocityHints
	Consistency      string
}

type ActionCounts struct {
	EventID       string `json:"event_id"`
	ReplyCount    int64  `json:"reply_count"`
	ReactionCount int64  `json:"reaction_count"`
	RepostCount   int64  `json:"repost_count"`
	Consistency   string `json:"consistency"`
}

type UserInfosResult struct {
	Profiles       []Profile
	MissingPubkeys []string
}

type SearchResult struct {
	Events   []json.RawMessage `json:"events"`
	Profiles []Profile         `json:"profiles"`
}

type SearchSuggestionsResult struct {
	Profiles []Profile           `json:"profiles"`
	Hashtags []HashtagSuggestion `json:"hashtags"`
}

type HashtagSuggestion struct {
	Hashtag       string `json:"hashtag"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

type NotesSearchParams struct {
	Query    string
	Limit    int
	Offset   int
	Sort     string
	Language string
	Window   *time.Duration
}

type ProfileSearchParams struct {
	Query  string
	Limit  int
	Offset int
	Sort   string
}

type TrustModeMetadata struct {
	TrustMode    string `json:"trust_mode"`
	TrustApplied bool   `json:"trust_applied"`
	ResultScope  string `json:"result_scope"`
}

type EventRepliesResult struct {
	EventID     string
	Replies     []json.RawMessage
	NextCursor  *EventCursor
	Consistency string
}

type EventAncestorsResult struct {
	EventID            string
	Ancestors          []json.RawMessage
	MissingAncestorIDs []string
	Consistency        string
}

type EventWithProvenanceResult struct {
	Event       json.RawMessage
	Relays      []model.EventRelay
	Consistency string
}

type EventWithProvenance struct {
	Event  json.RawMessage
	Relays []model.EventRelay
}

type EventCounts struct {
	EventID       string
	ReplyCount    int64
	ReactionCount int64
	RepostCount   int64
	Consistency   string
}

type EventSeenOnResult struct {
	EventID string
	SeenOn  []model.EventRelay
}

type NoteEngagementCounts struct {
	ReplyCount    int64 `json:"reply_count"`
	ReactionCount int64 `json:"reaction_count"`
	RepostCount   int64 `json:"repost_count"`
	ZapCount      int64 `json:"zap_count"`
	ZapMSats      int64 `json:"zap_msats"`
}

type NoteMediaFlags struct {
	HasImage        bool `json:"has_image"`
	HasVideo        bool `json:"has_video"`
	HasLink         bool `json:"has_link"`
	HasArticle      bool `json:"has_article"`
	AttachmentCount int  `json:"attachment_count"`
}

type NoteConversationActivity struct {
	Replies24h int64 `json:"replies_24h"`
	Replies7d  int64 `json:"replies_7d"`
}

type NoteSummary struct {
	EventID              string
	Event                json.RawMessage
	Author               ProfilePublicSummary
	Counts               NoteEngagementCounts
	Media                NoteMediaFlags
	RootEventID          *string
	ParentEventID        *string
	MissingAncestorIDs   []string
	ReferenceEventID     *string
	ReferenceEvent       json.RawMessage
	QuoteRepostLinkage   *NoteQuoteRepostLinkageSummary
	ConversationActivity *NoteConversationActivity
	Consistency          string
}

type NoteQuoteRepostLinkageSummary struct {
	EventID        string                `json:"event_id"`
	QuoteCount     int64                 `json:"quote_count"`
	RepostCount    int64                 `json:"repost_count"`
	RecentActivity []QuoteRepostActivity `json:"recent_activity"`
}

type RelatedNote struct {
	EventID      string
	AuthorPubkey string
	CreatedAt    int64
	Content      string
	Event        json.RawMessage
	Counts       NoteEngagementCounts
	Reasons      []string
	Score        int64
}
