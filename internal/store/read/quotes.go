package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// QuoteCountsByEventIDs counts kind-1 q-tag quotes of each event. Address
// quotes follow the current replaceable. The count is not part of repost_count.
func (s *Read) QuoteCountsByEventIDs(ctx context.Context, ids []string) (map[string]int64, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	trimmed := uniqueTrimmedIDs(ids)
	if len(trimmed) == 0 {
		return map[string]int64{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		WITH ids AS (
			SELECT DISTINCT btrim(id) AS event_id
			FROM unnest($1::text[]) AS id
			WHERE btrim(id) <> ''
		)
		SELECT ids.event_id, COUNT(DISTINCT q.source_event_id)::bigint
		FROM ids
		JOIN quote_contributions q ON (
			(q.target_event_id <> '' AND q.target_event_id = ids.event_id)
			OR (
				q.target_address <> ''
				AND q.target_event_id <> ids.event_id
				AND q.target_address = (
					SELECT rs.kind::text || ':' || lower(rs.pubkey) || ':' || rs.d_tag
					FROM replaceable_state rs
					WHERE rs.event_id = ids.event_id
				)
			)
		)
		GROUP BY ids.event_id
	`, topZapLookupIDs(trimmed))
	if err != nil {
		return nil, fmt.Errorf("quote counts by event ids: %w", err)
	}
	defer rows.Close()

	out := make(map[string]int64)
	for rows.Next() {
		var eventID string
		var count int64
		if err := rows.Scan(&eventID, &count); err != nil {
			return nil, fmt.Errorf("scan quote count: %w", err)
		}
		if count <= 0 {
			continue
		}
		out[eventID] = count
		out[strings.ToLower(eventID)] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read quote counts: %w", err)
	}
	return out, nil
}

func (s *Read) attachQuoteCounts(ctx context.Context, events []json.RawMessage) []json.RawMessage {
	if s == nil || s.pool == nil || len(events) == 0 {
		return events
	}
	ids := make([]string, 0, len(events))
	for _, raw := range events {
		if id := eventIDFromRaw(raw); id != "" {
			ids = append(ids, id)
		}
	}
	counts, err := s.QuoteCountsByEventIDs(ctx, ids)
	if err != nil || len(counts) == 0 {
		return events
	}
	out := make([]json.RawMessage, len(events))
	for i, raw := range events {
		id := eventIDFromRaw(raw)
		count := counts[id]
		if count == 0 {
			count = counts[strings.ToLower(id)]
		}
		if count <= 0 {
			out[i] = raw
			continue
		}
		merged, mergeErr := readmodel.MergeQuoteCountIntoRaw(raw, count)
		if mergeErr != nil || len(merged) == 0 {
			out[i] = raw
			continue
		}
		out[i] = merged
	}
	return out
}
