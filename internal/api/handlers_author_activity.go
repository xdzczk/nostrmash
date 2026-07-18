package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

// normalizePathPubkey normalizes hex or bech32 npub path params to lowercase hex.
// Invalid identifiers are returned trimmed (callers may still 404).
func normalizePathPubkey(raw string) string {
	raw = strings.TrimSpace(raw)
	if canonical := query.CanonicalizePubkey(raw); canonical != "" {
		return canonical
	}
	return raw
}

// normalizeAuthorPubkey is kept as an alias for author activity handlers.
func normalizeAuthorPubkey(raw string) string {
	return normalizePathPubkey(raw)
}

func (h Handlers) writeAuthorActivityPage(
	w http.ResponseWriter,
	r *http.Request,
	pubkey string,
	itemsKey string,
	items []json.RawMessage,
	nextCursor *query.EventCursor,
) {
	if items == nil {
		items = []json.RawMessage{}
	}
	nextCursorValue, err := encodeEventCursor(nextCursor)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := map[string]any{
		"pubkey":      pubkey,
		itemsKey:      items,
		"consistency": "eventual",
	}
	if nextCursorValue != "" {
		payload["next_cursor"] = nextCursorValue
	}
	addOpenTrustMetadata(payload)
	writeJSON(w, http.StatusOK, payload)
}

// GetAuthorZaps returns zap receipts sent by the given pubkey.
func (h Handlers) GetAuthorZaps(w http.ResponseWriter, r *http.Request) {
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

	result, err := h.service.GetAuthorSentZaps(r.Context(), pubkey, limit, cursor)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "unsupported", "author sent zaps are not available")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	h.writeAuthorActivityPage(w, r, result.Pubkey, "zaps", result.Zaps, result.NextCursor)
}

// GetAuthorReactions returns kind 7 reaction events authored by the given pubkey.
func (h Handlers) GetAuthorReactions(w http.ResponseWriter, r *http.Request) {
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

	result, err := h.service.GetAuthorReactions(r.Context(), pubkey, limit, cursor)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "unsupported", "author reactions are not available")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	h.writeAuthorActivityPage(w, r, result.Pubkey, "reactions", result.Reactions, result.NextCursor)
}
