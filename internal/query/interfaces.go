package query

import (
	"context"
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/store"
)

// ThreadService defines transport-agnostic thread assembly orchestration.
type ThreadService interface {
	GetThread(ctx context.Context, req ThreadRequest) (ThreadView, error)
	GetThreadWindow(ctx context.Context, req ThreadWindowRequest) (ThreadView, error)
}

// EventService defines transport-agnostic event read orchestration.
type EventService interface {
	GetEvent(ctx context.Context, id string) (json.RawMessage, error)
	GetEvents(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error)
}

// ProfileService defines transport-agnostic profile read orchestration.
type ProfileService interface {
	GetProfile(ctx context.Context, pubkey string) (store.ProfileProjection, error)
	GetProfiles(ctx context.Context, pubkeys []string) (UserInfosResult, error)
}

// ReadOrchestration groups focused read-side service capabilities.
type ReadOrchestration interface {
	ThreadService
	EventService
	ProfileService
}
