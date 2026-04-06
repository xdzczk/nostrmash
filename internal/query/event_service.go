package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

type eventService struct {
	reader EventReader
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
	return s.reader.GetEventRawByID(ctx, id)
}

func (s Service) GetEvents(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return s.GetEventBatch(ctx, ids)
}

func (s eventService) GetEvents(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	return s.reader.GetEventRawsByIDs(ctx, ids)
}

func (s Service) GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	return s.GetActionCounts(ctx, eventID)
}

func (s Service) GetEventByID(ctx context.Context, id string) (raw json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_by_id")
	defer func() { span.End(err) }()
	return s.reader.GetEventRawByID(ctx, id)
}

func (s Service) GetEventBatch(ctx context.Context, ids []string) (out map[string]json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_batch")
	defer func() { span.End(err) }()
	return s.reader.GetEventRawsByIDs(ctx, ids)
}

func (s Service) Search(ctx context.Context, text string, limit int) (out SearchResult, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.search")
	defer func() { span.End(err) }()
	text = strings.TrimSpace(text)
	if text == "" {
		return SearchResult{Events: []json.RawMessage{}, Profiles: []store.ProfileProjection{}}, nil
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
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
}

func (s Service) GetMentions(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetEventsReferencingPubkey(ctx, pubkey, limit)
}

func (s Service) GetFollowers(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetFollowersByPubkey(ctx, pubkey, limit)
}

func (s Service) GetZaps(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type receiverZapsReader interface {
		GetUserZaps(ctx context.Context, pubkey string, limit int, sortBySats bool) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(receiverZapsReader); ok {
		return r.GetUserZaps(ctx, pubkey, limit, false)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9735, pubkey, limit)
}

func (s Service) GetHighlights(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9802, pubkey, limit)
}

func (s Service) GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	type highlightsByEventReader interface {
		GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(highlightsByEventReader); ok {
		return r.GetHighlightsByEventID(ctx, eventID, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetHighlightsByATarget(
	ctx context.Context,
	kind int,
	pubkey string,
	identifier string,
	limit int,
) ([]json.RawMessage, error) {
	type highlightsByATargetReader interface {
		GetHighlightsByATarget(ctx context.Context, kind int, pubkey string, identifier string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(highlightsByATargetReader); ok {
		return r.GetHighlightsByATarget(ctx, kind, pubkey, identifier, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetUserZapsBySats(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type receiverZapsReader interface {
		GetUserZaps(ctx context.Context, pubkey string, limit int, sortBySats bool) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(receiverZapsReader); ok {
		return r.GetUserZaps(ctx, pubkey, limit, true)
	}
	return s.GetZaps(ctx, pubkey, limit)
}

func (s Service) GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	type eventZapsReader interface {
		GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.reader.(eventZapsReader); ok {
		return r.GetEventZapsBySats(ctx, eventID, limit)
	}
	return []json.RawMessage{}, nil
}
