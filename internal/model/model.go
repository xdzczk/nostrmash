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
// The original tag array is recoverable from events.raw_json; it is not
// duplicated onto each expanded row.
type EventTag struct {
	EventID    string
	TagName    string
	TagIndex   int
	ValueIndex int
	Value      string
}

// EventRelay is per-relay provenance.
type EventRelay struct {
	EventID  string
	RelayURL string
	SeenAt   time.Time
}

// FallbackRelayURL is the synthetic provenance written when API relay
// fallback persists an event that was not observed on a live subscription.
// It is not a connectable relay and must be excluded from public relay
// rankings and network stats.
const FallbackRelayURL = "fallback:relay"

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
	RelayURL           string
	Mode               string // live | backfill
	FilterGroup        string
	Since              *int64
	Until              *int64
	Cursor             *string
	LastEventID        *string
	LastProgressAt     *time.Time
	LastConnectedAt    *time.Time
	LastDisconnectedAt *time.Time
	EOSESeenAt         *time.Time
	Status             string
	LastError          *string
	LastErrorAt        *time.Time
	ReconnectCount     int
	UpdatedAt          time.Time
}

// IngestMode constants.
const (
	ModeLive     = "live"
	ModeBackfill = "backfill"
)

// Checkpoint status constants.
const (
	CheckpointRunning      = "running"
	CheckpointPaused       = "paused"
	CheckpointCompleted    = "completed"
	CheckpointFailed       = "failed"
	CheckpointConnecting   = "connecting"
	CheckpointHealthy      = "healthy"
	CheckpointBackingOff   = "backing_off"
	CheckpointErrored      = "errored"
	CheckpointDisconnected = "disconnected"
)
