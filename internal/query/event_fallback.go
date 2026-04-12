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
	allowed, strict := s.policy.eventAdmission(fallbackLookupDirect)
	if !allowed {
		return nil, err
	}
	maxAttempts, maxTimeBudget := s.policy.executionBounds(strict)

	started := time.Now()
	fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_event_by_id.fallback", traceutil.KV("fallback.surface", "event_by_id"))
	budgetCtx, cancel := withFallbackTimeBudget(fallbackCtx, maxTimeBudget)
	defer cancel()

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		observeFallbackAttemptByEntity(fallbackEntityEvent)
		foundByID, fallbackErr := s.fallback.FetchEventsByIDs(budgetCtx, []string{eventID})
		if fallbackErr != nil {
			lastErr = fallbackErr
			if budgetCtx.Err() != nil {
				break
			}
			continue
		}
		if raw, ok := foundByID[eventID]; ok {
			fallbackSpan.End(nil)
			observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultHit, time.Since(started))
			s.persistFallbackEvent(ctx, eventID, raw)
			return raw, nil
		}
		if budgetCtx.Err() != nil {
			break
		}
	}
	fallbackSpan.End(lastErr)
	if lastErr != nil {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultError, time.Since(started))
		logFallbackInfraFailure(ctx, "event_by_id", fallbackEntityEvent, eventID, lastErr, true)
		return nil, err
	}
	observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultMiss, time.Since(started))
	return nil, err
}

func (s eventService) persistFallbackEvent(_ context.Context, eventID string, raw json.RawMessage) {
	if s.persister == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.persister.PersistFallbackEvent(ctx, eventID, raw)
	}()
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
	allowed, strict := s.policy.eventAdmission(fallbackLookupDirect)
	if !allowed {
		return found, nil
	}
	maxAttempts, maxTimeBudget := s.policy.executionBounds(strict)

	started := time.Now()
	fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_event_batch.fallback", traceutil.KV("fallback.surface", "event_batch"))
	budgetCtx, cancel := withFallbackTimeBudget(fallbackCtx, maxTimeBudget)
	defer cancel()

	remaining := append([]string(nil), missing...)
	fallbackFound := make(map[string]json.RawMessage, len(missing))
	var lastErr error
	for attempt := 0; attempt < maxAttempts && len(remaining) > 0; attempt++ {
		observeFallbackAttemptByEntity(fallbackEntityEvent)
		attemptFound, fallbackErr := s.fallback.FetchEventsByIDs(budgetCtx, remaining)
		if fallbackErr != nil {
			lastErr = fallbackErr
			if budgetCtx.Err() != nil {
				break
			}
			continue
		}
		nextRemaining := make([]string, 0, len(remaining))
		for _, id := range remaining {
			raw, ok := attemptFound[id]
			if !ok {
				nextRemaining = append(nextRemaining, id)
				continue
			}
			fallbackFound[id] = raw
		}
		remaining = nextRemaining
		if budgetCtx.Err() != nil {
			break
		}
	}
	fallbackSpan.End(lastErr)
	if lastErr != nil {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultError, time.Since(started))
		logFallbackBatchInfraFailure(ctx, "event_batch", fallbackEntityEvent, missing, lastErr, true)
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
		s.persistFallbackEvent(ctx, id, raw)
	}
	return found, nil
}
