package httpx

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

var (
	ErrPayloadTooLarge = errors.New("payload too large")
	ErrInvalidJSON     = errors.New("invalid json")
	ErrMultipleJSON    = errors.New("multiple json values")
)

type DecodeJSONOptions struct {
	DisallowUnknown bool
}

// DecodeJSONBodyLimited decodes a single JSON value with optional size and schema strictness.
func DecodeJSONBodyLimited(
	w http.ResponseWriter,
	r *http.Request,
	maxBytes int64,
	dst any,
	opts DecodeJSONOptions,
) error {
	if maxBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	}
	decoder := json.NewDecoder(r.Body)
	if opts.DisallowUnknown {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(dst); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return ErrPayloadTooLarge
		}
		return ErrInvalidJSON
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return ErrMultipleJSON
	}
	return nil
}
