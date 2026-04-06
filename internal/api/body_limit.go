package api

import (
	"errors"
	"net/http"

	"github.com/xdzczk/nostrmash/internal/transport/httpx"
)

const (
	publicBatchBodyLimitBytes = 2 << 20
	adminBodyLimitBytes       = 256 << 10
)

func decodeJSONBodyLimited(
	w http.ResponseWriter,
	r *http.Request,
	maxBytes int64,
	dst any,
	disallowUnknown bool,
) bool {
	err := httpx.DecodeJSONBodyLimited(w, r, maxBytes, dst, httpx.DecodeJSONOptions{
		DisallowUnknown: disallowUnknown,
	})
	if err != nil {
		if errors.Is(err, httpx.ErrPayloadTooLarge) {
			writeError(r.Context(), w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds maximum size")
			return false
		}
		if errors.Is(err, httpx.ErrInvalidJSON) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
			return false
		}
		if errors.Is(err, httpx.ErrMultipleJSON) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object")
			return false
		}
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return false
	}
	return true
}
