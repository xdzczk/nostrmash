package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func clampPageLimit(limit int) int {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return limit
}

// GetAuthorMedia returns picture and video events for one pubkey. Addressable
// kinds are the current replaceable winner only.
func (s *Read) GetAuthorMedia(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM events e
		WHERE e.pubkey = $1
		  AND (
		      e.kind IN (20, 21, 22)
		      OR (
		          e.kind IN (34235, 34236)
		          AND EXISTS (SELECT 1 FROM replaceable_state rs WHERE rs.event_id = e.id)
		      )
		  )
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $2
	`, pubkey, clampPageLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("get author media: %w", err)
	}
	defer rows.Close()
	return scanRawText(rows)
}

// GetAuthorLive returns the current kind 30311 activities for one pubkey.
func (s *Read) GetAuthorLive(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM replaceable_state rs
		JOIN events e ON e.id = rs.event_id
		WHERE rs.pubkey = $1 AND rs.kind = 30311
		ORDER BY rs.created_at DESC, rs.event_id DESC
		LIMIT $2
	`, pubkey, clampPageLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("get author live: %w", err)
	}
	defer rows.Close()
	return scanRawText(rows)
}

// GetAuthorPins returns e and a values from the latest kind 10001 list.
func (s *Read) GetAuthorPins(ctx context.Context, pubkey string) ([]readmodel.PinItem, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT et.tag_name, et.value
		FROM replaceable_state rs
		JOIN event_tags et ON et.event_id = rs.event_id
		WHERE rs.pubkey = $1
		  AND rs.kind = 10001
		  AND et.tag_name IN ('e', 'a')
		  AND et.value <> ''
		  AND rs.event_id = (
		      SELECT event_id
		      FROM replaceable_state
		      WHERE pubkey = $1 AND kind = 10001
		      ORDER BY created_at DESC, event_id DESC
		      LIMIT 1
		  )
		ORDER BY et.tag_index ASC, et.value_index ASC
	`, pubkey)
	if err != nil {
		return nil, fmt.Errorf("get author pins: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.PinItem, 0)
	for rows.Next() {
		var item readmodel.PinItem
		if err := rows.Scan(&item.Tag, &item.Value); err != nil {
			return nil, fmt.Errorf("scan pin: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pins: %w", err)
	}
	return out, nil
}

// GetLiveActivities returns current kind 30311 rows whose status tag matches.
// followedBy empty is the global page. A pubkey limits the page to people that
// pubkey follows; an empty follow list returns no rows.
func (s *Read) GetLiveActivities(ctx context.Context, status, followedBy string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	status = strings.ToLower(strings.TrimSpace(status))
	if status == "" {
		status = "live"
	}
	followedBy = strings.TrimSpace(followedBy)
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM replaceable_state rs
		JOIN events e ON e.id = rs.event_id
		WHERE rs.kind = 30311
		  AND EXISTS (
		      SELECT 1
		      FROM event_tags st
		      WHERE st.event_id = e.id
		        AND st.tag_name = 'status'
		        AND lower(st.value) = $1
		  )
		  AND (
		      $2 = ''
		      OR e.pubkey IN (
		          SELECT followed_pubkey
		          FROM follower_edges
		          WHERE follower_pubkey = $2
		          LIMIT 2000
		      )
		  )
		ORDER BY COALESCE((
			SELECT CASE WHEN st.value ~ '^[0-9]+$' THEN st.value::bigint END
			FROM event_tags st
			WHERE st.event_id = e.id AND st.tag_name = 'starts'
			ORDER BY st.tag_index
			LIMIT 1
		), e.created_at) DESC, e.id DESC
		LIMIT $3
	`, status, followedBy, clampPageLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("get live activities: %w", err)
	}
	defer rows.Close()
	return scanRawText(rows)
}

// GetShortVideos returns kinds 22 and 34236, newest first. followedBy empty is
// the global page. Addressable shorts are the current replaceable winner.
func (s *Read) GetShortVideos(ctx context.Context, followedBy string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	followedBy = strings.TrimSpace(followedBy)
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM events e
		WHERE e.kind IN (22, 34236)
		  AND (
		      e.kind = 22
		      OR EXISTS (SELECT 1 FROM replaceable_state rs WHERE rs.event_id = e.id)
		  )
		  AND (
		      $1 = ''
		      OR e.pubkey IN (
		          SELECT followed_pubkey
		          FROM follower_edges
		          WHERE follower_pubkey = $1
		          LIMIT 2000
		      )
		  )
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $2
	`, followedBy, clampPageLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("get short videos: %w", err)
	}
	defer rows.Close()
	return scanRawText(rows)
}

// GetEventComments returns kind 1111 comments on an event id or its address.
func (s *Read) GetEventComments(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM comment_edges c
		JOIN events e ON e.id = c.source_event_id
		LEFT JOIN replaceable_state rs ON rs.event_id = $1
		WHERE c.root_event_id = $1
		   OR c.parent_event_id = $1
		   OR (
		        rs.event_id IS NOT NULL
		        AND (
		            c.root_address = rs.kind::text || ':' || rs.pubkey || ':' || rs.d_tag
		            OR c.parent_address = rs.kind::text || ':' || rs.pubkey || ':' || rs.d_tag
		        )
		   )
		ORDER BY c.created_at ASC, c.source_event_id ASC
		LIMIT $2
	`, eventID, clampPageLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("get event comments: %w", err)
	}
	defer rows.Close()
	return scanRawText(rows)
}

// GetEventReports returns NIP-56 reports whose e tag targets this event.
func (s *Read) GetEventReports(ctx context.Context, eventID string, limit int) ([]readmodel.ReportTarget, error) {
	return s.listReportTargets(ctx, "e", strings.TrimSpace(eventID), limit)
}

// GetUserReports returns NIP-56 reports whose p tag targets this pubkey.
func (s *Read) GetUserReports(ctx context.Context, pubkey string, limit int) ([]readmodel.ReportTarget, error) {
	return s.listReportTargets(ctx, "p", strings.TrimSpace(pubkey), limit)
}

func (s *Read) listReportTargets(ctx context.Context, tagName, target string, limit int) ([]readmodel.ReportTarget, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if target == "" {
		return nil, fmt.Errorf("report target is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT report_event_id, reporter_pubkey, report_type, content, created_at, target_value
		FROM report_targets
		WHERE tag_name = $1 AND target_value = $2
		ORDER BY created_at DESC, report_event_id DESC
		LIMIT $3
	`, tagName, target, clampPageLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("list report targets: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.ReportTarget, 0)
	for rows.Next() {
		var item readmodel.ReportTarget
		var value string
		if err := rows.Scan(&item.ReportEventID, &item.ReporterPubkey, &item.ReportType, &item.Content, &item.CreatedAt, &value); err != nil {
			return nil, fmt.Errorf("scan report target: %w", err)
		}
		if tagName == "p" {
			item.TargetPubkey = value
		} else {
			item.TargetEventID = value
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read report targets: %w", err)
	}
	return out, nil
}

func scanRawText(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}
	return out, nil
}
