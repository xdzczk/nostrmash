package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

// GetEventCounts returns eventually-consistent interaction counters from Layer 3 projections.
func (h Handlers) GetEventCounts(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	counts, err := h.service.GetEventActionCounts(r.Context(), eventID)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := map[string]any{
		"event_id":       counts.EventID,
		"reply_count":    counts.ReplyCount,
		"reaction_count": counts.ReactionCount,
		"repost_count":   counts.RepostCount,
		"zap_count":      counts.ZapCount,
		"zap_msats":      counts.ZapMSats,
		"consistency":    counts.Consistency,
	}
	if counts.QuoteCount > 0 {
		payload["quote_count"] = counts.QuoteCount
	}
	if breakdown := h.reactionContentBreakdown(r.Context(), eventID); len(breakdown) > 0 {
		payload["reaction_breakdown"] = breakdown
	}
	if h.pool != nil {
		var trusted, reactions, reposts, zaps int64
		if err := h.pool.QueryRow(r.Context(), `
			SELECT trusted_engagement_count, reaction_count, repost_count, zap_count
			FROM note_discovery_stats WHERE event_id = $1
		`, eventID).Scan(&trusted, &reactions, &reposts, &zaps); err == nil && reactions+reposts+zaps > 0 {
			payload["trusted_engagement_share"] = float64(trusted) / float64(reactions+reposts+zaps)
		}
	}
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) reactionContentBreakdown(ctx context.Context, eventID string) []map[string]any {
	if h.pool == nil {
		return nil
	}
	rows, err := h.pool.Query(ctx, `
		SELECT content_key, count
		FROM reaction_content_counts
		WHERE target_event_id = $1 AND count > 0
		ORDER BY count DESC, content_key ASC
		LIMIT 20
	`, eventID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var key string
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			return out
		}
		out = append(out, map[string]any{"content": key, "count": count})
	}
	return out
}

// GetEventReplies returns direct replies ordered by created_at asc, id asc with cursor pagination.
func (h Handlers) GetEventReplies(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cursor, err := decodeEventCursor(strings.TrimSpace(r.URL.Query().Get("cursor")))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
		return
	}

	repliesView, err := h.service.GetEventReplies(r.Context(), eventID, limit, cursor)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	nextCursor, err := encodeEventCursor(repliesView.NextCursor)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":    repliesView.EventID,
		"replies":     repliesView.Replies,
		"next_cursor": nextCursor,
		"consistency": repliesView.Consistency,
	})
}

// GetEventAncestors returns ancestors in root -> ... -> parent order.
func (h Handlers) GetEventAncestors(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	maxDepth, err := parseBoundedPositiveInt(r, "max_depth", 100, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	ancestorsView, err := h.service.GetEventAncestors(r.Context(), eventID, maxDepth)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":             ancestorsView.EventID,
		"ancestors":            ancestorsView.Ancestors,
		"missing_ancestor_ids": ancestorsView.MissingAncestorIDs,
		"consistency":          ancestorsView.Consistency,
	})
}

// GetThread returns a thread view for one event.
func (h Handlers) GetThread(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("eventId"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}

	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	maxDepth, err := parseBoundedPositiveInt(r, "max_depth", 100, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cursor, err := decodeEventCursor(strings.TrimSpace(r.URL.Query().Get("cursor")))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
		return
	}
	thread, err := h.service.GetThread(r.Context(), query.ThreadRequest{
		EventID:  eventID,
		Limit:    limit,
		MaxDepth: maxDepth,
		Cursor:   cursor,
	})
	if err != nil {
		if errors.Is(err, query.ErrThreadEventNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	nextCursor, err := encodeEventCursor(thread.NextCursor)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":             eventID,
		"event":                thread.Event,
		"ancestors":            thread.Ancestors,
		"missing_ancestor_ids": thread.MissingAncestorIDs,
		"replies":              thread.Replies,
		"next_cursor":          nextCursor,
		"consistency":          thread.Consistency,
	})
}

// GetThreadSummary returns projection-backed thread summary primitives for one root event.
func (h Handlers) GetThreadSummary(w http.ResponseWriter, r *http.Request) {
	rootEventID := strings.TrimSpace(r.PathValue("root_event_id"))
	if rootEventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "root event id is required")
		return
	}
	summary, err := h.service.GetThreadSummary(r.Context(), rootEventID)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "thread summary is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"root_event_id":     summary.RootEventID,
		"reply_count":       summary.ReplyCount,
		"participant_count": summary.ParticipantCount,
		"max_depth":         summary.MaxDepth,
		"last_activity_at":  summary.LastActivityAt,
		"conversation_velocity": map[string]any{
			"replies_24h": summary.Velocity.Replies24h,
			"replies_7d":  summary.Velocity.Replies7d,
			"replies_30d": summary.Velocity.Replies30d,
		},
		"consistency": summary.Consistency,
	})
}

// GetThreadActivity returns a velocity-oriented activity snapshot for one root thread.
func (h Handlers) GetThreadActivity(w http.ResponseWriter, r *http.Request) {
	rootEventID := strings.TrimSpace(r.PathValue("root_event_id"))
	if rootEventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "root event id is required")
		return
	}
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	summary, err := h.service.GetThreadSummary(r.Context(), rootEventID)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "thread activity is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	velocityScore := float64(summary.Velocity.Replies24h)
	switch window {
	case 7 * 24 * time.Hour:
		velocityScore = float64(summary.Velocity.Replies7d)
	case 30 * 24 * time.Hour:
		velocityScore = float64(summary.Velocity.Replies30d)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"root_event_id":     summary.RootEventID,
		"window":            windowLabel,
		"participant_count": summary.ParticipantCount,
		"last_activity_at":  summary.LastActivityAt,
		"activity": map[string]any{
			"replies_24h": summary.Velocity.Replies24h,
			"replies_7d":  summary.Velocity.Replies7d,
			"replies_30d": summary.Velocity.Replies30d,
		},
		"velocity_score": velocityScore,
		"consistency":    summary.Consistency,
	})
}
