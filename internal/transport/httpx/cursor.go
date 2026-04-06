package httpx

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// ErrCursorIDRequired indicates a cursor payload without an id.
var ErrCursorIDRequired = errors.New("cursor id is required")

// EventCursorPayload is the transport-neutral cursor payload schema.
type EventCursorPayload struct {
	CreatedAt int64  `json:"created_at"`
	ID        string `json:"id"`
}

// EncodeEventCursorPayload serializes a cursor payload for URL-safe transport.
func EncodeEventCursorPayload(payload EventCursorPayload) (string, error) {
	payload.ID = strings.TrimSpace(payload.ID)
	if payload.ID == "" && payload.CreatedAt == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

// DecodeEventCursorPayload parses a URL-safe encoded cursor payload.
func DecodeEventCursorPayload(value string) (*EventCursorPayload, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}
	var payload EventCursorPayload
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, err
	}
	payload.ID = strings.TrimSpace(payload.ID)
	if payload.ID == "" {
		return nil, ErrCursorIDRequired
	}
	return &payload, nil
}
