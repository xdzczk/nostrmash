package api

import (
	"net/http"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetTrustScore(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
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
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trust score is not available on this deployment")
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
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trust ranking is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scores": scores})
}
