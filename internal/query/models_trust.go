package query

import "time"

type TrustScore struct {
	Pubkey         string
	Score          float64
	Rank           int64
	RunID          int64
	DerivationName string
	TargetVersion  int
	ComputedAt     time.Time
}

type TrustState struct {
	Pubkey       string
	Score        *float64
	Qualified    bool
	Tier         string
	HopDistance  *int
	HopBucket    string
	Rank         *int64
	ComputedAt   *time.Time
	GenerationID *int64
	IsSeed       bool
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
