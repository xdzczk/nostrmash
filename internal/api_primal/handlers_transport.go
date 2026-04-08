package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/transport/httpx"
)

func parseBoundedPositiveInt(raw string, defaultValue int, maxValue int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return defaultValue
	}
	if parsed > maxValue {
		return maxValue
	}
	return parsed
}

func encodeEventCursor(cursor *query.EventCursor) (string, error) {
	if cursor == nil {
		return "", nil
	}
	return httpx.EncodeEventCursorPayload(httpx.EventCursorPayload{
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	})
}

func decodeEventCursor(value string) (*query.EventCursor, error) {
	payload, err := httpx.DecodeEventCursorPayload(value)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return &query.EventCursor{
		CreatedAt: payload.CreatedAt,
		ID:        payload.ID,
	}, nil
}

type apiErrorEnvelope struct {
	Error apiErrorBody `json:"error"`
}

type apiErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func writeError(ctx context.Context, w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiErrorEnvelope{
		Error: apiErrorBody{
			Code:      code,
			Message:   message,
			RequestID: logging.RequestIDFromContext(ctx),
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func normalizeUniqueValues(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}
