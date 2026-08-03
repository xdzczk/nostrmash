package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/traceutil"
)

// GetEventWithProvenance loads the canonical event payload and relay provenance.
func (s *PostgresStore) GetEventWithProvenance(ctx context.Context, id string) (EventWithProvenance, error) {
	out := EventWithProvenance{}
	raw, err := s.GetEventRawByID(ctx, id)
	if err != nil {
		return out, err
	}
	relays, err := s.GetEventSeenOn(ctx, id)
	if err != nil {
		return out, err
	}
	out.Event = raw
	out.Relays = relays
	return out, nil
}

// GetEventCounts returns eventually-consistent Layer 3 interaction counters.
func (s *PostgresStore) GetEventCounts(ctx context.Context, eventID string) (EventCounts, error) {
	out := EventCounts{Consistency: "eventual"}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return out, fmt.Errorf("event id is required")
	}
	out.EventID = eventID

	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(
		         (SELECT reply_count FROM thread_summaries WHERE root_event_id = $1),
		         (SELECT count FROM reply_counts WHERE event_id = $1),
		         0
		       ),
		       COALESCE((SELECT count FROM reaction_counts WHERE event_id = $1), 0),
		       COALESCE((SELECT count FROM repost_counts WHERE event_id = $1), 0),
		       COALESCE((SELECT zap_count FROM note_discovery_stats WHERE event_id = $1), 0),
		       COALESCE((SELECT zap_msats FROM note_discovery_stats WHERE event_id = $1), 0)
	`, eventID).Scan(&out.ReplyCount, &out.ReactionCount, &out.RepostCount, &out.ZapCount, &out.ZapMSats); err != nil {
		return out, fmt.Errorf("get event counts: %w", err)
	}
	return out, nil
}

// GetEventReplies returns one cursor-paginated page of direct replies ordered by created_at asc, id asc.
func (s *PostgresStore) GetEventReplies(
	ctx context.Context,
	eventID string,
	limit int,
	cursor *EventOrderCursor,
) (events []json.RawMessage, nextCursor *EventOrderCursor, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.get_event_replies")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("get_event_replies", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, ErrNotFound
	}

	type replyRow struct {
		id        string
		createdAt int64
		raw       string
	}
	rowsOut := make([]replyRow, 0, limit+1)

	var rows pgx.Rows
	if cursor == nil {
		rows, err = s.pool.Query(ctx, `
			SELECT te.child_event_id, te.child_created_at, e.raw_json::text
			FROM thread_edges te
			INNER JOIN events e ON e.id = te.child_event_id
			WHERE te.parent_event_id = $1
			ORDER BY te.child_created_at ASC, te.child_event_id ASC
			LIMIT $2
		`, eventID, limit+1)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT te.child_event_id, te.child_created_at, e.raw_json::text
			FROM thread_edges te
			INNER JOIN events e ON e.id = te.child_event_id
			WHERE te.parent_event_id = $1
			  AND (te.child_created_at, te.child_event_id) > ($2, $3)
			ORDER BY te.child_created_at ASC, te.child_event_id ASC
			LIMIT $4
		`, eventID, cursor.CreatedAt, cursor.ID, limit+1)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("get event replies: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row replyRow
		if err := rows.Scan(&row.id, &row.createdAt, &row.raw); err != nil {
			return nil, nil, fmt.Errorf("scan event replies row: %w", err)
		}
		rowsOut = append(rowsOut, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read event replies rows: %w", err)
	}

	hasMore := len(rowsOut) > limit
	if hasMore {
		rowsOut = rowsOut[:limit]
	}
	events = make([]json.RawMessage, 0, len(rowsOut))
	for _, row := range rowsOut {
		events = append(events, json.RawMessage(row.raw))
	}
	events, err = s.EnrichEventsWithCounts(ctx, events)
	if err != nil {
		return nil, nil, err
	}

	nextCursor = nil
	if hasMore && len(rowsOut) > 0 {
		last := rowsOut[len(rowsOut)-1]
		nextCursor = &EventOrderCursor{
			CreatedAt: last.createdAt,
			ID:        last.id,
		}
	}
	return events, nextCursor, nil
}

// GetEventRepliesDescending returns one cursor-paginated page of direct replies ordered by created_at desc, id desc.
// Cursor semantics match query descending windows: when cursor is provided, return replies strictly older than cursor.
func (s *PostgresStore) GetEventRepliesDescending(
	ctx context.Context,
	eventID string,
	limit int,
	cursor *EventOrderCursor,
	offset int,
) (events []json.RawMessage, nextCursor *EventOrderCursor, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.get_event_replies_descending")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("get_event_replies_descending", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		return []json.RawMessage{}, nil, nil
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, ErrNotFound
	}

	type replyRow struct {
		id        string
		createdAt int64
		raw       string
	}
	rowsOut := make([]replyRow, 0, limit+1)

	var rows pgx.Rows
	if cursor == nil {
		rows, err = s.pool.Query(ctx, `
			SELECT te.child_event_id, te.child_created_at, e.raw_json::text
			FROM thread_edges te
			INNER JOIN events e ON e.id = te.child_event_id
			WHERE te.parent_event_id = $1
			ORDER BY te.child_created_at DESC, te.child_event_id DESC
			OFFSET $2
			LIMIT $3
		`, eventID, offset, limit+1)
	} else {
		var cursorExists bool
		cursorID := strings.TrimSpace(cursor.ID)
		if cursorID == "" {
			return []json.RawMessage{}, nil, nil
		}
		if err := s.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM thread_edges te
				WHERE te.parent_event_id = $1
				  AND te.child_created_at = $2
				  AND te.child_event_id = $3
			)
		`, eventID, cursor.CreatedAt, cursorID).Scan(&cursorExists); err != nil {
			return nil, nil, fmt.Errorf("lookup descending cursor existence: %w", err)
		}
		if !cursorExists {
			return []json.RawMessage{}, nil, nil
		}
		rows, err = s.pool.Query(ctx, `
			SELECT te.child_event_id, te.child_created_at, e.raw_json::text
			FROM thread_edges te
			INNER JOIN events e ON e.id = te.child_event_id
			WHERE te.parent_event_id = $1
			  AND (te.child_created_at, te.child_event_id) < ($2, $3)
			ORDER BY te.child_created_at DESC, te.child_event_id DESC
			LIMIT $4
		`, eventID, cursor.CreatedAt, cursorID, limit+1)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("get event replies descending: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row replyRow
		if err := rows.Scan(&row.id, &row.createdAt, &row.raw); err != nil {
			return nil, nil, fmt.Errorf("scan event replies descending row: %w", err)
		}
		rowsOut = append(rowsOut, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read event replies descending rows: %w", err)
	}

	hasMore := len(rowsOut) > limit
	if hasMore {
		rowsOut = rowsOut[:limit]
	}
	events = make([]json.RawMessage, 0, len(rowsOut))
	for _, row := range rowsOut {
		events = append(events, json.RawMessage(row.raw))
	}
	events, err = s.EnrichEventsWithCounts(ctx, events)
	if err != nil {
		return nil, nil, err
	}

	if hasMore && len(rowsOut) > 0 {
		last := rowsOut[len(rowsOut)-1]
		nextCursor = &EventOrderCursor{
			CreatedAt: last.createdAt,
			ID:        last.id,
		}
	}
	return events, nextCursor, nil
}

// GetEventAncestors returns ancestors ordered root -> ... -> parent for one event.
func (s *PostgresStore) GetEventAncestors(
	ctx context.Context,
	eventID string,
	maxDepth int,
) (ancestors []json.RawMessage, missingIDs []string, err error) {
	started := time.Now()
	ctx, span := traceutil.StartSpan(ctx, "store.get_event_ancestors")
	defer func() {
		span.End(err)
		metrics.ObserveDBOperation("get_event_ancestors", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil, fmt.Errorf("event id is required")
	}
	if maxDepth <= 0 {
		maxDepth = 100
	}
	if maxDepth > 200 {
		maxDepth = 200
	}

	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, ErrNotFound
	}

	ancestorIDs := make([]string, 0, maxDepth)
	missingSet := map[string]struct{}{}
	current := eventID
	visited := map[string]struct{}{
		eventID: {},
	}

	for i := 0; i < maxDepth; i++ {
		var parentID string
		var parentMissing bool
		err := s.pool.QueryRow(ctx, `
			SELECT parent_event_id, parent_missing
			FROM thread_edges
			WHERE child_event_id = $1
		`, current).Scan(&parentID, &parentMissing)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				break
			}
			return nil, nil, fmt.Errorf("lookup thread edge for ancestors: %w", err)
		}
		parentID = strings.TrimSpace(parentID)
		if parentID == "" {
			break
		}
		if _, seen := visited[parentID]; seen {
			break
		}
		visited[parentID] = struct{}{}
		ancestorIDs = append(ancestorIDs, parentID)
		if parentMissing {
			missingSet[parentID] = struct{}{}
			break
		}
		current = parentID
	}

	foundByID, err := s.GetEventRawsByIDs(ctx, ancestorIDs)
	if err != nil {
		return nil, nil, err
	}

	ancestors = make([]json.RawMessage, 0, len(ancestorIDs))
	for i := len(ancestorIDs) - 1; i >= 0; i-- {
		ancestorID := ancestorIDs[i]
		raw, ok := foundByID[ancestorID]
		if !ok {
			missingSet[ancestorID] = struct{}{}
			continue
		}
		ancestors = append(ancestors, raw)
	}
	missingIDs = make([]string, 0, len(missingSet))
	for id := range missingSet {
		missingIDs = append(missingIDs, id)
	}
	slices.Sort(missingIDs)
	return ancestors, missingIDs, nil
}

// GetThreadSummary returns projection-backed root-level thread summary primitives.
func (s *PostgresStore) GetThreadSummary(ctx context.Context, rootEventID string) (ThreadSummaryProjection, error) {
	out := ThreadSummaryProjection{Consistency: "eventual"}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	rootEventID = strings.TrimSpace(rootEventID)
	if rootEventID == "" {
		return out, fmt.Errorf("root event id is required")
	}
	out.RootEventID = rootEventID
	var kind int
	if err := s.pool.QueryRow(ctx, `
		SELECT kind
		FROM events
		WHERE id = $1
	`, rootEventID).Scan(&kind); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, ErrNotFound
		}
		return out, fmt.Errorf("load thread summary root event: %w", err)
	}
	if kind != 1 {
		return out, ErrNotFound
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(ts.reply_count, 0),
			COALESCE(ts.participant_count, 1),
			COALESCE(ts.max_depth, 0),
			COALESCE(ts.last_activity_at, e.created_at),
			COALESCE(ts.replies_24h, 0),
			COALESCE(ts.replies_7d, 0)
		FROM events e
		LEFT JOIN thread_summaries ts ON ts.root_event_id = e.id
		WHERE e.id = $1
	`, rootEventID).Scan(
		&out.ReplyCount,
		&out.ParticipantCount,
		&out.MaxDepth,
		&out.LastActivityAt,
		&out.Replies24h,
		&out.Replies7d,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, ErrNotFound
		}
		return out, fmt.Errorf("get thread summary: %w", err)
	}
	return out, nil
}
