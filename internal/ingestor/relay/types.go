package relay

import (
	"context"
	"time"
)

// State describes a relay connection lifecycle phase.
type State string

const (
	StateConnecting State = "connecting"
	StateHealthy    State = "healthy"
	StateLagging    State = "lagging"
	StateBackingOff State = "backing_off"
	StateDisabled   State = "disabled"
	StateErrored    State = "errored"
)

// Status is the internal relay lifecycle snapshot for metrics/admin use.
type Status struct {
	URL          string
	State        State
	Since        time.Time
	LastProgress time.Time
	LastError    string
	Attempts     int
	Backoff      time.Duration
}

// Config controls relay lifecycle behavior.
type Config struct {
	Relays []string

	Allowlist      []string
	DisabledRelays []string

	ConnectTimeout time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	LagThreshold   time.Duration

	StatusSink RelayStatusSink
}

// RelayStatusSink records durable relay lifecycle status transitions.
type RelayStatusSink interface {
	SetRelayStatus(ctx context.Context, relayURL string, state State, lastError string) error
}

// Connection represents an active relay connection.
type Connection interface {
	// Done closes when the connection drops. Err may be nil for clean close.
	Done() <-chan error
	Messages() <-chan []byte
	Close() error
}

// Connector establishes relay connections.
type Connector interface {
	Connect(ctx context.Context, relayURL string) (Connection, error)
}

// SinceResolution describes a computed live subscription cursor.
type SinceResolution struct {
	Since                    int64
	Strategy                 string
	CheckpointSince          *int64
	BootstrapLookbackSeconds int64
	OverlapSeconds           int64
}

// SinceResolver computes per-relay "since" values for live subscriptions.
type SinceResolver interface {
	ResolveSince(ctx context.Context, relayURL string) (SinceResolution, error)
}
