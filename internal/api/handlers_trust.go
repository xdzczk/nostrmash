package api

import (
	"net/http"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetTrustScore(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	score, err := h.service.GetTrustScore(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "trust score not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, score)
}

func (h Handlers) ListTopTrustScores(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 500)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	scores, err := h.service.ListTopTrustedPubkeys(r.Context(), limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scores": scores})
}
