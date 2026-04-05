package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
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
	if maxBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	}
	decoder := json.NewDecoder(r.Body)
	if disallowUnknown {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(dst); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeError(r.Context(), w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds maximum size")
			return false
		}
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object")
		return false
	}
	return true
}
