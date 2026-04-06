package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

// GetEventCounts returns eventually-consistent interaction counters from Layer 3 projections.
func (h Handlers) GetEventCounts(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	counts, err := h.store.GetEventCounts(r.Context(), eventID)
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

	replies, next, err := h.store.GetEventReplies(r.Context(), eventID, limit, cursor)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	nextCursor, err := encodeEventCursor(next)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":    eventID,
		"replies":     replies,
		"next_cursor": nextCursor,
		"consistency": "eventual",
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

	ancestors, missing, err := h.store.GetEventAncestors(r.Context(), eventID, maxDepth)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":             eventID,
		"ancestors":            ancestors,
		"missing_ancestor_ids": missing,
		"consistency":          "eventual",
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
