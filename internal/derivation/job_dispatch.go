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
	case JobTypeDeriveEventBundle:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.DeriveEventBundle(ctx, payload.EventID)
	case JobTypeDeriveEventRelationships:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.DeriveEventRelationships(ctx, payload.EventID)
	case JobTypeUpdateReplaceableState:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.UpdateReplaceableState(ctx, payload.EventID)
	case JobTypeProjectProfilesLatest:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectProfilesLatest(ctx, payload.EventID)
	case JobTypeProjectAuthorRecentEvent:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectAuthorRecentEvent(ctx, payload.EventID)
	case JobTypeProjectReplyCounts:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectReplyCounts(ctx, payload.EventID)
	case JobTypeProjectReactionCounts:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectReactionCounts(ctx, payload.EventID)
	case JobTypeProjectRepostCounts:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectRepostCounts(ctx, payload.EventID)
	case JobTypeProjectReactionEvents:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectReactionEvents(ctx, payload.EventID)
	case JobTypeProjectRepostEvents:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectRepostEvents(ctx, payload.EventID)
	case JobTypeProjectDeletionEvents:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectDeletionEvents(ctx, payload.EventID)
	case JobTypeProjectContactLists:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectContactListsLatest(ctx, payload.EventID)
	case JobTypeProjectRelayLists:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectRelayListsLatest(ctx, payload.EventID)
	case JobTypeProjectDMUnreadCounts:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectDMUnreadCounts(ctx, payload.EventID)
	case JobTypeProjectZapReceipts:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.ProjectZapReceipts(ctx, payload.EventID)
	case JobTypeUpdateThreadProjection:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.UpdateThreadProjection(ctx, payload.EventID)
	case JobTypeRepairUnresolvedRefs:
		payload, err := decodeEventJobPayload(job.Payload)
		if err != nil {
			return err
		}
		return handlers.RepairUnresolvedReferences(ctx, payload.EventID)
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
