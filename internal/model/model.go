package model

import (
	"encoding/json"
	"time"
)

// Event is Layer 1 canonical row.
type Event struct {
	ID          string
	Pubkey      string
	CreatedAt   int64
	Kind        int
	Sig         string
	Content     string
	RawJSON     json.RawMessage
	FirstSeenAt time.Time
	InsertedAt  time.Time
}

// EventTag is one expanded tag value.
type EventTag struct {
	EventID    string
	TagName    string
	TagIndex   int
	ValueIndex int
	Value      string
	RawValues  json.RawMessage
}

// EventRelay is per-relay provenance.
type EventRelay struct {
	EventID  string
	RelayURL string
	SeenAt   time.Time
}

// InvalidEvent is quarantine record.
type InvalidEvent struct {
	ID           string
	SourceRelay  string
	ErrorCode    string
	ErrorMessage string
	RawPayload   json.RawMessage
	SeenAt       time.Time
}

// IngestCheckpoint persists per-relay ingest state.
type IngestCheckpoint struct {
	RelayURL    string
	Mode        string // live | backfill
	FilterGroup string
	Since       *int64
	Until       *int64
	Cursor      *string
	EOSESeenAt  *time.Time
	Status      string // running | paused | completed | failed
	UpdatedAt   time.Time
}

// IngestMode constants.
const (
	ModeLive     = "live"
	ModeBackfill = "backfill"
)

// Checkpoint status constants.
const (
	CheckpointRunning   = "running"
	CheckpointPaused    = "paused"
	CheckpointCompleted = "completed"
	CheckpointFailed    = "failed"
)
