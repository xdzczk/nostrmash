package query

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

func (s eventService) getEventWithFallback(ctx context.Context, eventID string) (json.RawMessage, error) {
	raw, err := s.reader.GetEventRawByID(ctx, eventID)
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
	observeFallbackAttemptByEntity(fallbackEntityEvent)
	fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_event_by_id.fallback", traceutil.KV("fallback.surface", "event_by_id"))
	foundByID, fallbackErr := s.fallback.FetchEventsByIDs(fallbackCtx, []string{eventID})
	fallbackSpan.End(fallbackErr)
	if fallbackErr != nil {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultError, time.Since(started))
		logFallbackInfraFailure(ctx, "event_by_id", fallbackEntityEvent, eventID, fallbackErr, true)
		return nil, err
	}
	raw, ok := foundByID[eventID]
	if !ok {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultMiss, time.Since(started))
		return nil, err
	}
	observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultHit, time.Since(started))
	return raw, nil
}

func (s eventService) mergeEventsWithFallback(ctx context.Context, normalizedIDs []string, found map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	missing := make([]string, 0)
	for _, id := range normalizedIDs {
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
	observeFallbackAttemptByEntity(fallbackEntityEvent)
	fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_event_batch.fallback", traceutil.KV("fallback.surface", "event_batch"))
	fallbackFound, fallbackErr := s.fallback.FetchEventsByIDs(fallbackCtx, missing)
	fallbackSpan.End(fallbackErr)
	if fallbackErr != nil {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultError, time.Since(started))
		logFallbackBatchInfraFailure(ctx, "event_batch", fallbackEntityEvent, missing, fallbackErr, true)
		return found, nil
	}
	if len(fallbackFound) == 0 {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultMiss, time.Since(started))
		return found, nil
	}
	recovered := 0
	for _, id := range missing {
		if _, ok := fallbackFound[id]; ok {
			recovered++
		}
	}
	if recovered == 0 {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultMiss, time.Since(started))
		return found, nil
	}
	observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultHit, time.Since(started))
	for id, raw := range fallbackFound {
		found[id] = raw
	}
	return found, nil
}
