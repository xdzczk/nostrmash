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

type TrustScore struct {
	Pubkey         string
	Score          float64
	Rank           int64
	RunID          int64
	DerivationName string
	TargetVersion  int
	ComputedAt     time.Time
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
