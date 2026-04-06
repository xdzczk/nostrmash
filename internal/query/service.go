package query

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

type Reader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventWithProvenance(ctx context.Context, id string) (store.EventWithProvenance, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error)
	GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error)
	GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error)
	GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error)
	GetEventReplies(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
	ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error)
	GetContactListByPubkey(ctx context.Context, pubkey string) (store.ContactListProjection, error)
	GetRelayListByPubkey(ctx context.Context, pubkey string) (store.RelayListProjection, error)
	SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error)
	SearchProfiles(ctx context.Context, query string, limit int) ([]store.ProfileProjection, error)
	GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error)
	GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
	GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error)
}

// ThreadReader is the minimal dependency needed for thread assembly orchestration.
type ThreadReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventReplies(ctx context.Context, eventID string, limit int, cursor *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error)
}

// EventReader is the minimal dependency needed for event lookup/count orchestration.
type EventReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error)
}

// ProfileReader is the minimal dependency needed for profile/user-info orchestration.
type ProfileReader interface {
	GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error)
	GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error)
}

type Service struct {
	reader Reader
}

func NewService(reader Reader) Service {
	return Service{reader: reader}
}

var (
	_ ThreadService     = Service{}
	_ EventService      = Service{}
	_ ProfileService    = Service{}
	_ ReadOrchestration = Service{}

	// ErrThreadEventNotFound indicates the focal/root event for a thread was not found.
	// This is intentionally narrower than store.ErrNotFound so transports can preserve
	// historical status code behavior for non-root thread fetch failures.
	ErrThreadEventNotFound = errors.New("thread event not found")
)

func normalizeUniqueStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func IsNotFound(err error) bool {
	return errors.Is(err, store.ErrNotFound)
}
