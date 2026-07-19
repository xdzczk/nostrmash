package derivation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Job is the minimal job envelope required for derivation dispatch.
type Job struct {
	JobType string
	Payload json.RawMessage
}

// eventJobHandlers maps every job type whose payload is a single {event_id}
// envelope to the handler method that processes it. Using a registry keeps the
// dispatch table declarative — adding a projection means adding one row rather
// than another near-identical decode+call case in a long switch. Method
// expressions capture the receiver so the handler is bound at dispatch time.
var eventJobHandlers = map[string]func(*Handlers, context.Context, string) error{
	JobTypeDeriveEventBundle:        (*Handlers).DeriveEventBundle,
	JobTypeDeriveEventRelationships: (*Handlers).DeriveEventRelationships,
	JobTypeUpdateReplaceableState:   (*Handlers).UpdateReplaceableState,
	JobTypeProjectProfilesLatest:    (*Handlers).ProjectProfilesLatest,
	JobTypeProjectAuthorRecentEvent: (*Handlers).ProjectAuthorRecentEvent,
	JobTypeProjectReplyCounts:       (*Handlers).ProjectReplyCounts,
	JobTypeProjectReactionCounts:    (*Handlers).ProjectReactionCounts,
	JobTypeProjectRepostCounts:      (*Handlers).ProjectRepostCounts,
	JobTypeProjectReactionEvents:    (*Handlers).ProjectReactionEvents,
	JobTypeProjectRepostEvents:      (*Handlers).ProjectRepostEvents,
	JobTypeProjectDeletionEvents:    (*Handlers).ProjectDeletionEvents,
	JobTypeProjectContactLists:      (*Handlers).ProjectContactListsLatest,
	JobTypeProjectRelayLists:        (*Handlers).ProjectRelayListsLatest,
	JobTypeProjectDMUnreadCounts:    (*Handlers).ProjectDMUnreadCounts,
	JobTypeProjectZapReceipts:       (*Handlers).ProjectZapReceipts,
	JobTypeUpdateThreadProjection:   (*Handlers).UpdateThreadProjection,
	JobTypeRepairUnresolvedRefs:     (*Handlers).RepairUnresolvedReferences,
}

// ProcessJob executes one derivation job payload using the same logic as cmd/worker.
func ProcessJob(ctx context.Context, handlers *Handlers, job Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if handlers == nil {
		return fmt.Errorf("derivation handlers are not configured")
	}
	if handle, ok := eventJobHandlers[job.JobType]; ok {
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handle(handlers, ctx, payload.EventID)
	}
	switch job.JobType {
	case JobTypeRebuildProjectionScope:
		var payload RebuildProjectionScopeJobPayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return fmt.Errorf("decode rebuild scope payload: %w", err)
		}
		if payload.RunID <= 0 {
			return fmt.Errorf("run_id is required in payload")
		}
		return handlers.ExecuteProjectionRebuildRun(ctx, payload.RunID)
	default:
		return fmt.Errorf("job type %q not implemented", job.JobType)
	}
}

func decodeEventJobPayload(raw json.RawMessage) (EventJobPayload, error) {
	var payload EventJobPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return payload, fmt.Errorf("decode job payload: %w", err)
	}
	payload.EventID = strings.TrimSpace(payload.EventID)
	if payload.EventID == "" {
		return payload, fmt.Errorf("event_id is required in payload")
	}
	return payload, nil
}
