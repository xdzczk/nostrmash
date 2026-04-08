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
	Query  string
	Limit  int
	Offset int
	Sort   string
	Window *time.Duration
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

type Profile struct {
	Pubkey            string
	MetadataEventID   string
	MetadataCreatedAt int64
	ProfileJSON       json.RawMessage
}

type ProfilePublicStats struct {
	Pubkey           string
	FollowerCount    int64
	FollowingCount   int64
	NoteCount        int64
	ReplyCount       int64
	RecentActivityAt *int64
}

type ProfilePublicSummary struct {
	Profile Profile
	Stats   ProfilePublicStats
}

type ContactList struct {
	Pubkey          string
	EventID         string
	CreatedAt       int64
	DerivationVer   int
	ContactsJSONRaw json.RawMessage
}

type RelayList struct {
	Pubkey        string
	EventID       string
	CreatedAt     int64
	DerivationVer int
	RelaysJSONRaw json.RawMessage
}

type NetworkStats struct {
	Events   int64 `json:"events"`
	Profiles int64 `json:"profiles"`
	Relays   int64 `json:"relays"`
}

type WindowedCount struct {
	Last24h int64 `json:"24h"`
	Last7d  int64 `json:"7d"`
}

type TrendingHashtagWindows struct {
	Last24h []TrendingHashtag `json:"24h"`
	Last7d  []TrendingHashtag `json:"7d"`
}

type PublicDiscoveryNetworkStats struct {
	EventsIngested    int64                   `json:"events_ingested"`
	ProjectedProfiles int64                   `json:"projected_profiles"`
	Relays            int64                   `json:"relays"`
	ActiveAuthors     WindowedCount           `json:"active_authors"`
	NoteVolume        WindowedCount           `json:"note_volume"`
	TopHashtags       *TrendingHashtagWindows `json:"top_hashtags,omitempty"`
}

type CuratedRecommendedRead struct {
	EventID string `json:"event_id"`
	Title   string `json:"title"`
	URL     string `json:"url"`
	Rank    int    `json:"rank"`
}

type CuratedReadsTopic struct {
	Topic string `json:"topic"`
	Rank  int    `json:"rank"`
}

type CuratedFeaturedAuthor struct {
	Pubkey string `json:"pubkey"`
	Rank   int    `json:"rank"`
}

type TrendingHashtag struct {
	Hashtag       string `json:"hashtag"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

type TrendingNote struct {
	EventID       string  `json:"event_id"`
	AuthorPubkey  string  `json:"author_pubkey"`
	CreatedAt     int64   `json:"created_at"`
	Content       string  `json:"content"`
	ReplyCount    int64   `json:"reply_count"`
	RepostCount   int64   `json:"repost_count"`
	ReactionCount int64   `json:"reaction_count"`
	ZapCount      int64   `json:"zap_count"`
	ZapMSats      int64   `json:"zap_msats"`
	Score         float64 `json:"score"`
}

type TrendingProfile struct {
	Pubkey                   string  `json:"pubkey"`
	Score                    float64 `json:"score"`
	RecentPostCount          int64   `json:"recent_post_count"`
	RecentReplyCount         int64   `json:"recent_reply_count"`
	RecentEngagementReceived int64   `json:"recent_engagement_received"`
	RecentZapVolumeMSats     int64   `json:"recent_zap_volume_msats"`
	RecentActiveDays         int     `json:"recent_active_days"`
	RecentActivityAt         *int64  `json:"recent_activity_at,omitempty"`
}

type TrustScore struct {
	Pubkey         string
	Score          float64
	Rank           int64
	RunID          int64
	DerivationName string
	TargetVersion  int
	ComputedAt     time.Time
}

type TrustQualificationPolicy struct {
	MaxHops      int
	MinimumScore float64
}

type TrustQualification struct {
	Pubkey       string
	Trusted      bool
	IsSeed       bool
	DistanceHops *int
	Score        *float64
	Rank         *int64
	SourceRunID  *int64
}

type TrustRun struct {
	ID                 int64
	DerivationName     string
	TargetVersion      int
	Status             string
	JobID              *int64
	Attempts           int
	InputFollowerEdges int64
	ScoreRowsPublished int64
	RedisSnapshotRef   *string
	CurrentPhase       *string
	SyncJobID          *int64
	ComputeJobID       *int64
	PromoteJobID       *int64
	PhaseStartedAt     *time.Time
	PhaseFinishedAt    *time.Time
	PhaseLastError     *string
	StartedAt          *time.Time
	FinishedAt         *time.Time
	LastError          *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}
