package query

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/traceutil"
)

// authorEventsFallbackReader is the optional relay-fallback capability for
// fetching an author's recent events. Satisfied by the relaylookup client
// through the readmodel fallback adapter.
type authorEventsFallbackReader interface {
	FetchEventsByAuthor(ctx context.Context, pubkey string, kinds []int, limit int) ([]json.RawMessage, error)
}

// thinProfileFallbackKinds are the note-like kinds fetched for the general
// (kind-agnostic) author-events surface: short notes and long-form articles.
var thinProfileFallbackKinds = []int{1, 30023}

type authorEventEnvelope struct {
	ID        string `json:"id"`
	Pubkey    string `json:"pubkey"`
	CreatedAt int64  `json:"created_at"`
}

// maybeAugmentAuthorEventsFromRelays runs the thin-profile fallback: when the
// local author-events read returned fewer rows than the configured threshold,
// fetch the author's recent events from the fallback relays (bounded by the
// shared fallback time budget), merge them with the local rows, and persist
// the newly discovered events asynchronously so they enter derivation and
// future local reads. Returns the merged rows (never fewer than local).
func (s Service) maybeAugmentAuthorEventsFromRelays(
	ctx context.Context,
	pubkey string,
	kinds []int,
	limit int,
	local []json.RawMessage,
) []json.RawMessage {
	threshold := s.authorEventsFallbackMinResults
	if threshold <= 0 || s.fallback == nil {
		return local
	}
	if threshold > limit {
		threshold = limit
	}
	if len(local) >= threshold {
		return local
	}
	fetcher, ok := s.fallback.(authorEventsFallbackReader)
	if !ok {
		return local
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return local
	}
	policy := s.fallbackPolicy()
	admitted, _ := policy.admitProfiles(ctx, []string{pubkey}, fallbackLookupThinProfile)
	if len(admitted) == 0 {
		return local
	}
	strict := policy.mode != trustModeOpen
	maxAttempts, maxTimeBudget := policy.executionBounds(strict)

	started := time.Now()
	fallbackCtx, fallbackSpan := traceutil.StartSpan(
		ctx,
		"query.get_author_events.fallback",
		traceutil.KV("fallback.surface", "author_events"),
	)
	budgetCtx, cancel := withFallbackTimeBudget(fallbackCtx, maxTimeBudget)
	defer cancel()

	var fetched []json.RawMessage
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		observeFallbackAttemptByEntity(fallbackEntityEvent)
		rows, fetchErr := fetcher.FetchEventsByAuthor(budgetCtx, pubkey, kinds, limit)
		if fetchErr != nil {
			if IsUnsupportedCapability(fetchErr) {
				fallbackSpan.End(nil)
				return local
			}
			lastErr = fetchErr
			if budgetCtx.Err() != nil {
				break
			}
			continue
		}
		lastErr = nil
		fetched = rows
		break
	}
	fallbackSpan.End(lastErr)
	if lastErr != nil {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultError, time.Since(started))
		logFallbackInfraFailure(ctx, "author_events", fallbackEntityEvent, pubkey, lastErr, false)
		return local
	}
	if len(fetched) == 0 {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultMiss, time.Since(started))
		return local
	}

	merged, discovered := mergeAuthorEvents(local, fetched, pubkey, limit)
	if len(discovered) == 0 {
		observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultMiss, time.Since(started))
		return local
	}
	observeFallbackResultByEntity(fallbackEntityEvent, fallbackResultHit, time.Since(started))
	s.persistFallbackAuthorEvents(discovered)
	return merged
}

type discoveredAuthorEvent struct {
	id  string
	raw json.RawMessage
}

// mergeAuthorEvents combines local rows with relay-fetched rows, deduplicated
// by event id, sorted by created_at desc then id desc, trimmed to limit. The
// second return value lists fetched events that were not already stored
// locally (candidates for fallback persistence).
func mergeAuthorEvents(
	local []json.RawMessage,
	fetched []json.RawMessage,
	pubkey string,
	limit int,
) ([]json.RawMessage, []discoveredAuthorEvent) {
	type mergedRow struct {
		id        string
		createdAt int64
		raw       json.RawMessage
	}
	rows := make([]mergedRow, 0, len(local)+len(fetched))
	seen := make(map[string]struct{}, len(local)+len(fetched))
	localIDs := make(map[string]struct{}, len(local))

	appendRow := func(raw json.RawMessage, requireAuthor bool) (string, bool) {
		var envelope authorEventEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return "", false
		}
		id := strings.TrimSpace(envelope.ID)
		if id == "" {
			return "", false
		}
		if requireAuthor && !strings.EqualFold(strings.TrimSpace(envelope.Pubkey), pubkey) {
			return "", false
		}
		if _, exists := seen[id]; exists {
			return "", false
		}
		seen[id] = struct{}{}
		rows = append(rows, mergedRow{id: id, createdAt: envelope.CreatedAt, raw: raw})
		return id, true
	}

	for _, raw := range local {
		if id, ok := appendRow(raw, false); ok {
			localIDs[id] = struct{}{}
		}
	}
	discovered := make([]discoveredAuthorEvent, 0, len(fetched))
	for _, raw := range fetched {
		id, ok := appendRow(raw, true)
		if !ok {
			continue
		}
		if _, existsLocally := localIDs[id]; !existsLocally {
			discovered = append(discovered, discoveredAuthorEvent{id: id, raw: raw})
		}
	}

	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].createdAt != rows[j].createdAt {
			return rows[i].createdAt > rows[j].createdAt
		}
		return rows[i].id > rows[j].id
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.raw)
	}
	return out, discovered
}

// persistFallbackAuthorEvents saves relay-discovered author events in the
// background so they enter derivation and future local reads. Mirrors
// eventService.persistFallbackEvent.
func (s Service) persistFallbackAuthorEvents(discovered []discoveredAuthorEvent) {
	if s.fallbackEventPersister == nil || len(discovered) == 0 {
		return
	}
	events := append([]discoveredAuthorEvent(nil), discovered...)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, event := range events {
			if err := s.fallbackEventPersister.PersistFallbackEvent(ctx, event.id, event.raw); err != nil {
				slog.Warn("persist_fallback_author_event_failed",
					"event_id", event.id,
					"error", err.Error(),
				)
			}
		}
	}()
}
