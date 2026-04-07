package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
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
	raw, err := s.reader.GetEventRawByID(ctx, trimmedID)
	if err == nil {
		metrics.ObserveLookupLocal("event_by_id", true)
		return raw, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	metrics.ObserveLookupLocal("event_by_id", false)
	if s.fallback == nil {
		return nil, err
	}

	started := time.Now()
	metrics.IncLookupFallbackAttempt("event_by_id")
	fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_event_by_id.fallback", traceutil.KV("fallback.surface", "event_by_id"))
	foundByID, fallbackErr := s.fallback.FetchEventsByIDs(fallbackCtx, []string{trimmedID})
	fallbackSpan.End(fallbackErr)
	metrics.ObserveLookupFallbackLatency("event_by_id", time.Since(started))
	if fallbackErr != nil {
		metrics.IncLookupFallbackFailure("event_by_id")
		return nil, err
	}
	raw, ok := foundByID[trimmedID]
	if !ok {
		metrics.IncLookupFallbackMiss("event_by_id")
		return nil, err
	}
	metrics.IncLookupFallbackSuccess("event_by_id")
	return raw, nil
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

	missing := make([]string, 0)
	for _, id := range normalized {
		if _, ok := found[id]; ok {
			continue
		}
		missing = append(missing, id)
	}
	if len(missing) == 0 {
		metrics.ObserveLookupLocal("event_batch", true)
		return found, nil
	}
	metrics.ObserveLookupLocal("event_batch", false)
	if s.fallback == nil {
		return found, nil
	}

	started := time.Now()
	metrics.IncLookupFallbackAttempt("event_batch")
	fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_event_batch.fallback", traceutil.KV("fallback.surface", "event_batch"))
	fallbackFound, fallbackErr := s.fallback.FetchEventsByIDs(fallbackCtx, missing)
	fallbackSpan.End(fallbackErr)
	metrics.ObserveLookupFallbackLatency("event_batch", time.Since(started))
	if fallbackErr != nil {
		metrics.IncLookupFallbackFailure("event_batch")
		return found, nil
	}
	if len(fallbackFound) == 0 {
		metrics.IncLookupFallbackMiss("event_batch")
		return found, nil
	}
	recovered := 0
	for _, id := range missing {
		if _, ok := fallbackFound[id]; ok {
			recovered++
		}
	}
	if recovered == 0 {
		metrics.IncLookupFallbackMiss("event_batch")
		return found, nil
	}
	if recovered < len(missing) {
		metrics.IncLookupFallbackPartialSuccess("event_batch")
	} else {
		metrics.IncLookupFallbackSuccess("event_batch")
	}
	for id, raw := range fallbackFound {
		found[id] = raw
	}
	return found, nil
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
