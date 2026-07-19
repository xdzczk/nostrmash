package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type eventService struct {
	reader    EventReader
	fallback  FallbackReader
	persister FallbackEventPersister
	policy    fallbackPolicyRuntime
}

// NewEventService constructs an event-only orchestration service from a narrow dependency.
func NewEventService(reader EventReader) EventService {
	return eventService{reader: reader}
}

func (s eventService) GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return ActionCounts{}, fmt.Errorf("event id is required")
	}
	counts, err := s.reader.GetEventCounts(ctx, eventID)
	if err != nil {
		return ActionCounts{}, err
	}
	return ActionCounts{
		EventID:       counts.EventID,
		ReplyCount:    counts.ReplyCount,
		ReactionCount: counts.ReactionCount,
		RepostCount:   counts.RepostCount,
		ZapCount:      counts.ZapCount,
		ZapMSats:      counts.ZapMSats,
		Consistency:   counts.Consistency,
	}, nil
}

func (s eventService) GetEventByID(ctx context.Context, id string) (json.RawMessage, error) {
	trimmedID := strings.TrimSpace(id)
	if trimmedID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	return s.getEventWithFallback(ctx, trimmedID)
}

func (s eventService) GetEventBatch(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	normalized := normalizeUniqueStrings(ids)
	if len(normalized) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	found, err := s.reader.GetEventRawsByIDs(ctx, normalized)
	if err != nil {
		return nil, err
	}
	return s.mergeEventsWithFallback(ctx, normalized, found)
}
