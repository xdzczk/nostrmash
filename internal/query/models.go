package query

import (
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/store"
)

// ThreadRequest captures transport-agnostic inputs for assembling one thread view.
type ThreadRequest struct {
	EventID  string
	Limit    int
	MaxDepth int
	Cursor   *store.EventOrderCursor
}

// ThreadWindowRequest captures transport-agnostic inputs for descending-window thread lookups.
type ThreadWindowRequest struct {
	EventID  string
	Limit    int
	MaxDepth int
	Cursor   *store.EventOrderCursor
	Offset   int
}

type ThreadView struct {
	Event              json.RawMessage
	Ancestors          []json.RawMessage
	MissingAncestorIDs []string
	Replies            []json.RawMessage
	NextCursor         *store.EventOrderCursor
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
	Profiles       []store.ProfileProjection
	MissingPubkeys []string
}

type SearchResult struct {
	Events   []json.RawMessage         `json:"events"`
	Profiles []store.ProfileProjection `json:"profiles"`
}
