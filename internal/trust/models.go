package trust

import "time"

const (
	RunStatusPending   = "pending"
	RunStatusRunning   = "running"
	RunStatusSucceeded = "succeeded"
	RunStatusFailed    = "failed"
)

const (
	RunPhaseSync    = "sync"
	RunPhaseCompute = "compute"
	RunPhasePromote = "promote"
)

type Run struct {
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

type ComputeGlobalScoresPayload struct {
	RunID            int64  `json:"run_id"`
	RedisSnapshotRef string `json:"redis_snapshot_ref,omitempty"`
}

type SyncGraphRedisPayload struct {
	RunID int64 `json:"run_id"`
}

type PromoteRunPayload struct {
	RunID            int64  `json:"run_id"`
	RedisSnapshotRef string `json:"redis_snapshot_ref,omitempty"`
}
