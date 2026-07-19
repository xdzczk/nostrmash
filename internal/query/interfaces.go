package query

import (
	"context"
	"encoding/json"
)

// ThreadService defines transport-agnostic thread assembly orchestration.
type ThreadService interface {
	GetThread(ctx context.Context, req ThreadRequest) (ThreadView, error)
	GetThreadWindow(ctx context.Context, req ThreadWindowRequest) (ThreadView, error)
}

// EventService defines transport-agnostic event read orchestration.
type EventService interface {
	GetEventByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventBatch(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error)
}

// ProfileService defines transport-agnostic profile read orchestration.
type ProfileService interface {
	GetProfile(ctx context.Context, pubkey string) (Profile, error)
	GetProfiles(ctx context.Context, pubkeys []string) (UserInfosResult, error)
	GetProfilePublicSummary(ctx context.Context, pubkey string) (ProfilePublicSummary, error)
}

type TrustService interface {
	GetTrustState(ctx context.Context, pubkey string) (TrustState, error)
	GetTrustStates(ctx context.Context, pubkeys []string) (map[string]TrustState, error)
	GetTrustScore(ctx context.Context, pubkey string) (TrustScore, error)
	ListTopTrustedPubkeys(ctx context.Context, limit int) ([]TrustScore, error)
	GetTrustRun(ctx context.Context, runID int64) (TrustRun, error)
	ListTrustRuns(ctx context.Context, limit int) ([]TrustRun, error)
	IsTrustedAuthor(ctx context.Context, pubkey string, policy TrustQualificationPolicy) (bool, error)
	GetTrustQualification(ctx context.Context, pubkeys []string, policy TrustQualificationPolicy) (map[string]TrustQualification, error)
}

// ReadOrchestration groups focused read-side service capabilities.
type ReadOrchestration interface {
	ThreadService
	EventService
	ProfileService
	TrustService
}
