package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// GetAuthorEvents returns projected recent events sorted by created_at desc, id desc.
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
	kindRaw := strings.TrimSpace(r.URL.Query().Get("kind"))
	var events []json.RawMessage
	if kindRaw != "" {
		var kind int
		kind, err = strconv.Atoi(kindRaw)
		if err != nil || kind < 0 {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "kind must be a non-negative integer")
			return
		}
		events, err = h.service.GetAuthorEventsByKind(r.Context(), pubkey, kind, limit)
	} else {
		events, err = h.service.GetAuthorEvents(r.Context(), pubkey, limit)
	}
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey": pubkey,
		"events": events,
	})
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
