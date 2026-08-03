package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func (s Service) GetEventReplies(ctx context.Context, eventID string, limit int, cursor *EventCursor) (EventRepliesResult, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EventRepliesResult{}, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	replies, nextCursor, err := s.reader.GetEventReplies(ctx, eventID, limit, cursor)
	if err != nil {
		return EventRepliesResult{}, err
	}
	return EventRepliesResult{
		EventID:     eventID,
		Replies:     replies,
		NextCursor:  nextCursor,
		Consistency: "eventual",
	}, nil
}

func (s Service) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) (EventAncestorsResult, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EventAncestorsResult{}, fmt.Errorf("event id is required")
	}
	if maxDepth <= 0 {
		maxDepth = 100
	}
	if maxDepth > 100 {
		maxDepth = 100
	}
	ancestors, missing, err := s.reader.GetEventAncestors(ctx, eventID, maxDepth)
	if err != nil {
		return EventAncestorsResult{}, err
	}
	return EventAncestorsResult{
		EventID:            eventID,
		Ancestors:          ancestors,
		MissingAncestorIDs: missing,
		Consistency:        "eventual",
	}, nil
}

func (s Service) GetEventWithProvenance(ctx context.Context, eventID string) (EventWithProvenanceResult, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EventWithProvenanceResult{}, fmt.Errorf("event id is required")
	}
	event, err := s.reader.GetEventWithProvenance(ctx, eventID)
	if err == nil {
		relays := make([]model.EventRelay, 0, len(event.Relays))
		for _, relay := range event.Relays {
			relays = append(relays, model.EventRelay{
				EventID:  relay.EventID,
				RelayURL: relay.RelayURL,
				SeenAt:   relay.SeenAt.UTC(),
			})
		}
		return EventWithProvenanceResult{
			Event:       s.enrichEventRawWithCounts(ctx, eventID, event.Event),
			Relays:      relays,
			Consistency: "strong",
		}, nil
	}
	if !errors.Is(err, readmodel.ErrNotFound) {
		return EventWithProvenanceResult{}, err
	}
	raw, fallbackErr := eventService{reader: s.reader, fallback: s.fallback, persister: s.fallbackEventPersister, policy: s.fallbackPolicy()}.GetEventByID(ctx, eventID)
	if fallbackErr != nil {
		return EventWithProvenanceResult{}, readmodel.ErrNotFound
	}
	return EventWithProvenanceResult{
		Event:       s.enrichEventRawWithCounts(ctx, eventID, raw),
		Relays:      []model.EventRelay{},
		Consistency: "eventual",
	}, nil
}

func (s Service) enrichEventRawWithCounts(ctx context.Context, eventID string, raw json.RawMessage) json.RawMessage {
	counts, err := s.reader.GetEventCounts(ctx, eventID)
	if err != nil {
		return raw
	}
	enriched, err := readmodel.MergeEventCountsIntoRaw(raw, readmodel.EventCounts{
		EventID:       counts.EventID,
		ReplyCount:    counts.ReplyCount,
		ReactionCount: counts.ReactionCount,
		RepostCount:   counts.RepostCount,
		ZapCount:      counts.ZapCount,
		ZapMSats:      counts.ZapMSats,
		Consistency:   counts.Consistency,
	})
	if err != nil {
		return raw
	}
	return enriched
}

func (s Service) GetEventSeenOn(ctx context.Context, eventID string) (EventSeenOnResult, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EventSeenOnResult{}, fmt.Errorf("event id is required")
	}
	seenOn, err := s.reader.GetEventSeenOn(ctx, eventID)
	if err != nil {
		return EventSeenOnResult{}, err
	}
	out := make([]model.EventRelay, 0, len(seenOn))
	for _, relay := range seenOn {
		out = append(out, model.EventRelay{
			EventID:  relay.EventID,
			RelayURL: relay.RelayURL,
			SeenAt:   relay.SeenAt.UTC(),
		})
	}
	return EventSeenOnResult{
		EventID: eventID,
		SeenOn:  out,
	}, nil
}
