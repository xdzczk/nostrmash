package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

// GetEventRawByID returns the canonical Layer 1 event JSON by id.
func (s *PostgresStore) GetEventRawByID(ctx context.Context, id string) (rawJSON json.RawMessage, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.get_event_raw_by_id")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("get_event_raw_by_id", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	trimmedID := strings.TrimSpace(id)
	if trimmedID == "" {
		return nil, fmt.Errorf("event id is required")
	}

	var raw string
	err = s.pool.QueryRow(ctx, `
		SELECT raw_json::text
		FROM events
		WHERE id = $1
	`, trimmedID).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get event by id: %w", err)
	}
	return json.RawMessage(raw), nil
}

// GetEventRawsByIDs fetches canonical Layer 1 event JSON by ids.
func (s *PostgresStore) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if len(ids) == 0 {
		return map[string]json.RawMessage{}, nil
	}

	trimmedIDs := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		trimmedID := strings.TrimSpace(id)
		if trimmedID == "" {
			continue
		}
		if _, ok := seen[trimmedID]; ok {
			continue
		}
		seen[trimmedID] = struct{}{}
		trimmedIDs = append(trimmedIDs, trimmedID)
	}
	if len(trimmedIDs) == 0 {
		return map[string]json.RawMessage{}, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, raw_json::text
		FROM events
		WHERE id = ANY($1::text[])
	`, trimmedIDs)
	if err != nil {
		return nil, fmt.Errorf("get events by ids: %w", err)
	}
	defer rows.Close()

	out := make(map[string]json.RawMessage, len(trimmedIDs))
	for rows.Next() {
		var id string
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		out[id] = json.RawMessage(raw)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event rows: %w", err)
	}
	return out, nil
}

// GetEventSeenOn returns relay provenance rows for an event id.
func (s *PostgresStore) GetEventSeenOn(ctx context.Context, id string) ([]model.EventRelay, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	trimmedID := strings.TrimSpace(id)
	if trimmedID == "" {
		return nil, fmt.Errorf("event id is required")
	}

	rows, err := s.pool.Query(ctx, `
		SELECT relay_url, seen_at
		FROM event_relays
		WHERE event_id = $1
		ORDER BY seen_at ASC, relay_url ASC
	`, trimmedID)
	if err != nil {
		return nil, fmt.Errorf("get event seen-on: %w", err)
	}
	defer rows.Close()

	relays := make([]model.EventRelay, 0)
	for rows.Next() {
		var relay model.EventRelay
		relay.EventID = trimmedID
		if err := rows.Scan(&relay.RelayURL, &relay.SeenAt); err != nil {
			return nil, fmt.Errorf("scan event relay row: %w", err)
		}
		relays = append(relays, relay)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event relay rows: %w", err)
	}
	if len(relays) > 0 {
		return relays, nil
	}

	var exists bool
	err = s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)
	`, trimmedID).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("check event existence: %w", err)
	}
	if !exists {
		return nil, ErrNotFound
	}
	return relays, nil
}
