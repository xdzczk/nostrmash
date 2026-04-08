package api

import (
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
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":       counts.EventID,
		"reply_count":    counts.ReplyCount,
		"reaction_count": counts.ReactionCount,
		"repost_count":   counts.RepostCount,
		"consistency":    counts.Consistency,
	})
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
	if window == 7*24*time.Hour {
		velocityScore = float64(summary.Velocity.Replies7d)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"root_event_id":     summary.RootEventID,
		"window":            windowLabel,
		"participant_count": summary.ParticipantCount,
		"last_activity_at":  summary.LastActivityAt,
		"activity": map[string]any{
			"replies_24h": summary.Velocity.Replies24h,
			"replies_7d":  summary.Velocity.Replies7d,
		},
		"velocity_score": velocityScore,
		"consistency":    summary.Consistency,
	})
}
