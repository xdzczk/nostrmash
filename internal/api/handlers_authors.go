package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// GetAuthorEvents returns projected recent events sorted by created_at desc,
// id desc, with keyset cursor pagination via `cursor`/`next_cursor`.
func (h Handlers) GetAuthorEvents(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizeAuthorPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
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
	kindRaw := strings.TrimSpace(r.URL.Query().Get("kind"))
	var kind *int
	if kindRaw != "" {
		parsedKind, kindErr := strconv.Atoi(kindRaw)
		if kindErr != nil || parsedKind < 0 {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "kind must be a non-negative integer")
			return
		}
		kind = &parsedKind
	}
	result, err := h.service.GetAuthorEventsPage(r.Context(), pubkey, kind, limit, cursor)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	events := result.Events
	if events == nil {
		events = []json.RawMessage{}
	}
	nextCursorValue, err := encodeEventCursor(result.NextCursor)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := map[string]any{
		"pubkey": pubkey,
		"events": events,
	}
	if nextCursorValue != "" {
		payload["next_cursor"] = nextCursorValue
	}
	writeJSON(w, http.StatusOK, payload)
}

// GetAuthorReplies returns replies authored by pubkey.
func (h Handlers) GetAuthorReplies(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizeAuthorPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	replies, err := h.service.GetAuthorReplies(r.Context(), pubkey, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey": pubkey,
		"items":  replies,
	})
}
