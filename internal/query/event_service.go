package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

type eventService struct {
	reader   EventReader
	fallback FallbackReader
}

// NewEventService constructs an event-only orchestration service from a narrow dependency.
func NewEventService(reader EventReader) EventService {
	return eventService{reader: reader}
}

func (s Service) GetActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	return eventService{reader: s.reader}.GetEventActionCounts(ctx, eventID)
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
		Consistency:   counts.Consistency,
	}, nil
}

func (s Service) GetEvent(ctx context.Context, id string) (json.RawMessage, error) {
	return s.GetEventByID(ctx, id)
}

func (s eventService) GetEvent(ctx context.Context, id string) (json.RawMessage, error) {
	trimmedID := strings.TrimSpace(id)
	if trimmedID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	return s.getEventWithFallback(ctx, trimmedID)
}

func (s Service) GetEvents(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return s.GetEventBatch(ctx, ids)
}

func (s eventService) GetEvents(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
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

func (s Service) GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	return s.GetActionCounts(ctx, eventID)
}

func (s Service) GetEventByID(ctx context.Context, id string) (raw json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_by_id")
	defer func() { span.End(err) }()
	return eventService{reader: s.reader, fallback: s.fallback}.GetEvent(ctx, id)
}

func (s Service) GetEventBatch(ctx context.Context, ids []string) (out map[string]json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_batch")
	defer func() { span.End(err) }()
	return eventService{reader: s.reader, fallback: s.fallback}.GetEvents(ctx, ids)
}

func (s Service) Search(ctx context.Context, text string, limit int) (out SearchResult, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search")
	defer func() { span.End(err) }()
	text = strings.TrimSpace(text)
	if text == "" {
		return SearchResult{Events: []json.RawMessage{}, Profiles: []Profile{}}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	events, err := s.reader.SearchEventsByContent(ctx, text, limit)
	if err != nil {
		return SearchResult{}, err
	}
	profiles, err := s.reader.SearchProfiles(ctx, text, limit)
	if err != nil {
		return SearchResult{}, err
	}
	return SearchResult{Events: events, Profiles: profiles}, nil
}

func (s Service) GetAuthorEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetAuthorRecentEvents(ctx, pubkey, limit)
}

func (s Service) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetAuthorReplies(ctx, pubkey, limit)
}

func (s Service) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
}

func (s Service) GetMentions(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetEventsReferencingPubkey(ctx, pubkey, limit)
}

func (s Service) GetFollowers(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetFollowersByPubkey(ctx, pubkey, limit)
}

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
			Event:       event.Event,
			Relays:      relays,
			Consistency: "strong",
		}, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return EventWithProvenanceResult{}, err
	}
	raw, fallbackErr := eventService{reader: s.reader, fallback: s.fallback}.GetEvent(ctx, eventID)
	if fallbackErr != nil {
		return EventWithProvenanceResult{}, store.ErrNotFound
	}
	return EventWithProvenanceResult{
		Event:       raw,
		Relays:      []model.EventRelay{},
		Consistency: "eventual",
	}, nil
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

func (s Service) GetRelaysHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	rows, err := s.reader.ListRelayHealth(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]model.IngestCheckpoint, 0, len(rows))
	for _, row := range rows {
		checkpoint := row
		checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
		if checkpoint.EOSESeenAt != nil {
			eoseSeenAt := checkpoint.EOSESeenAt.UTC()
			checkpoint.EOSESeenAt = &eoseSeenAt
		}
		out = append(out, checkpoint)
	}
	return out, nil
}

func (s Service) GetZaps(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.userZaps; r != nil {
		return r.GetUserZaps(ctx, pubkey, limit, false)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9735, pubkey, limit)
}

func (s Service) GetHighlights(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9802, pubkey, limit)
}

func (s Service) GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.highlightsByEventID; r != nil {
		return r.GetHighlightsByEventID(ctx, eventID, limit)
	}
	return nil, unsupportedCapabilityError("highlights by event id")
}

func (s Service) GetHighlightsByATarget(
	ctx context.Context,
	kind int,
	pubkey string,
	identifier string,
	limit int,
) ([]json.RawMessage, error) {
	if r := s.capabilities.event.highlightsByATarget; r != nil {
		return r.GetHighlightsByATarget(ctx, kind, pubkey, identifier, limit)
	}
	return nil, unsupportedCapabilityError("highlights by a-target")
}

func (s Service) GetUserZapsBySats(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.userZaps; r != nil {
		return r.GetUserZaps(ctx, pubkey, limit, true)
	}
	return s.GetZaps(ctx, pubkey, limit)
}

func (s Service) GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.eventZapsBySats; r != nil {
		return r.GetEventZapsBySats(ctx, eventID, limit)
	}
	return nil, unsupportedCapabilityError("event zaps by sats")
}
