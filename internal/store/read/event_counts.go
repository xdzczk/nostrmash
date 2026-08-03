package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// GetEventCountsByIDs returns eventually-consistent engagement counters keyed by event id.
func (s *Read) GetEventCountsByIDs(ctx context.Context, ids []string) (map[string]readmodel.EventCounts, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	trimmed := uniqueTrimmedIDs(ids)
	if len(trimmed) == 0 {
		return map[string]readmodel.EventCounts{}, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			x.id,
			COALESCE(
				(SELECT reply_count FROM thread_summaries WHERE root_event_id = x.id),
				(SELECT count FROM reply_counts WHERE event_id = x.id),
				0
			),
			COALESCE((SELECT count FROM reaction_counts WHERE event_id = x.id), 0),
			COALESCE((SELECT count FROM repost_counts WHERE event_id = x.id), 0),
			COALESCE((SELECT zap_count FROM note_discovery_stats WHERE event_id = x.id), 0),
			COALESCE((SELECT zap_msats FROM note_discovery_stats WHERE event_id = x.id), 0)
		FROM unnest($1::text[]) AS x(id)
	`, trimmed)
	if err != nil {
		return nil, fmt.Errorf("get event counts by ids: %w", err)
	}
	defer rows.Close()

	out := make(map[string]readmodel.EventCounts, len(trimmed))
	for rows.Next() {
		var counts readmodel.EventCounts
		counts.Consistency = "eventual"
		if err := rows.Scan(
			&counts.EventID,
			&counts.ReplyCount,
			&counts.ReactionCount,
			&counts.RepostCount,
			&counts.ZapCount,
			&counts.ZapMSats,
		); err != nil {
			return nil, fmt.Errorf("scan event counts by ids row: %w", err)
		}
		out[counts.EventID] = counts
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event counts by ids rows: %w", err)
	}
	return out, nil
}

// EnrichEventRawMap attaches engagement counters to each raw event in the map.
func (s *Read) EnrichEventRawMap(ctx context.Context, byID map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	if len(byID) == 0 {
		return byID, nil
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	countsByID, err := s.GetEventCountsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[string]json.RawMessage, len(byID))
	for id, raw := range byID {
		counts := countsByID[id]
		counts.EventID = id
		if counts.Consistency == "" {
			counts.Consistency = "eventual"
		}
		enriched, err := readmodel.MergeEventCountsIntoRaw(raw, counts)
		if err != nil {
			return nil, err
		}
		out[id] = enriched
	}
	return out, nil
}

// EnrichEventsWithCounts attaches engagement counters to a list of raw events.
// Event ids are read from each payload's top-level "id" field.
func (s *Read) EnrichEventsWithCounts(ctx context.Context, events []json.RawMessage) ([]json.RawMessage, error) {
	if len(events) == 0 {
		return events, nil
	}
	ids := make([]string, 0, len(events))
	for _, raw := range events {
		if id := eventIDFromRaw(raw); id != "" {
			ids = append(ids, id)
		}
	}
	countsByID, err := s.GetEventCountsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]json.RawMessage, 0, len(events))
	for _, raw := range events {
		id := eventIDFromRaw(raw)
		counts := countsByID[id]
		counts.EventID = id
		if counts.Consistency == "" {
			counts.Consistency = "eventual"
		}
		enriched, err := readmodel.MergeEventCountsIntoRaw(raw, counts)
		if err != nil {
			return nil, err
		}
		out = append(out, enriched)
	}
	return out, nil
}

func eventIDFromRaw(raw json.RawMessage) string {
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.ID)
}

func uniqueTrimmedIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}
