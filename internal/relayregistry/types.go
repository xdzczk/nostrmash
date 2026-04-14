package relayregistry

import (
	"encoding/json"
	"time"
)

// ManualPolicy controls operator-driven relay overrides.
type ManualPolicy string

const (
	ManualPolicyNone    ManualPolicy = "none"
	ManualPolicyPinned  ManualPolicy = "pinned"
	ManualPolicyBlocked ManualPolicy = "blocked"
	ManualPolicyDrained ManualPolicy = "drained"
)

func (p ManualPolicy) Valid() bool {
	switch p {
	case ManualPolicyNone, ManualPolicyPinned, ManualPolicyBlocked, ManualPolicyDrained:
		return true
	}
	return false
}

// AdmissionState tracks the relay's lifecycle in the admission pipeline.
type AdmissionState string

const (
	AdmissionCandidate AdmissionState = "candidate"
	AdmissionProbation AdmissionState = "probation"
	AdmissionActive    AdmissionState = "active"
	AdmissionInactive  AdmissionState = "inactive"
	AdmissionBlocked   AdmissionState = "blocked"
	AdmissionDraining  AdmissionState = "draining"
	AdmissionPinned    AdmissionState = "pinned"
)

// ProbeStatus classifies the outcome of a relay health probe.
type ProbeStatus string

const (
	ProbeStatusOK              ProbeStatus = "ok"
	ProbeStatusConnectFailed   ProbeStatus = "connect_failed"
	ProbeStatusSubscribeFailed ProbeStatus = "subscribe_failed"
	ProbeStatusEOSETimeout     ProbeStatus = "eose_timeout"
	ProbeStatusProtocolError   ProbeStatus = "protocol_error"
	ProbeStatusRateLimited     ProbeStatus = "rate_limited"
	ProbeStatusUnknownError    ProbeStatus = "unknown_error"
)

// RelayRecord is the persistent registry row for a single relay.
type RelayRecord struct {
	URLKey        string
	NormalizedURL string

	DiscoveredAt time.Time
	LastSeenAt   time.Time

	SourceSeed     bool
	SourceUserList bool
	SourceManual   bool

	ManualPolicy   ManualPolicy
	AdmissionState AdmissionState

	Score                float64
	DistinctUserRefCount int
	WeightedUserRefScore float64

	LastProbeAt       *time.Time
	LastProbeStatus   *ProbeStatus
	LastConnectOK     *bool
	LastSubscribeOK   *bool
	LastEOSEOK        *bool
	AvgConnectLatency *float64
	AvgEOSELatency    *float64
	ProbeFailRate     float64
	YieldScore        float64
	DuplicateRatio    float64

	ScoreComponents   json.RawMessage
	CapabilitySummary json.RawMessage
	Notes             json.RawMessage

	UpdatedAt time.Time
}

// ProbeObservation is a single probe result row.
type ProbeObservation struct {
	ID               int64
	URLKey           string
	ProbedAt         time.Time
	ConnectOK        bool
	SubscribeOK      bool
	EOSEOK           bool
	ConnectLatencyMs *float64
	EOSELatencyMs    *float64
	ErrorCode        *string
	ErrorTextShort   *string
	SampleYieldCount *int
	SampleDupRatio   *float64
	CapabilityJSON   json.RawMessage
}

// DesiredSetSnapshot is a published desired active relay set.
type DesiredSetSnapshot struct {
	ID          int64
	PublishedAt time.Time
	RelayURLs   []string
	Source      string
	Notes       string
}

// ListFilter controls relay registry list queries.
type ListFilter struct {
	AdmissionStates []AdmissionState
	ManualPolicies  []ManualPolicy
	Limit           int
}
