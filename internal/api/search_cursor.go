package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

// searchCursorVersion guards cursor payload evolution: unknown versions are
// rejected as malformed rather than misinterpreted.
const searchCursorVersion = 1

// errSearchCursorMismatch indicates a cursor issued for different search
// parameters (query text, sort, window, or language).
var errSearchCursorMismatch = errors.New("cursor does not match the current search parameters")

// searchCursorPayload is the opaque continuation token for paginated search.
// Relevance-sorted searches carry only Offset (relevance ranking has no
// stable keyset); latest-sorted note searches additionally carry the
// (CreatedAt, ID) keyset of the last returned row so keyset-capable readers
// can resume without deep OFFSET scans. Offset is always present as the
// fallback position for offset-only readers.
type searchCursorPayload struct {
	V         int    `json:"v"`
	QHash     string `json:"q"`
	Sort      string `json:"sort"`
	Offset    int    `json:"offset"`
	CreatedAt int64  `json:"created_at,omitempty"`
	ID        string `json:"id,omitempty"`
}

// searchCursorScopeHash fingerprints the search parameters a cursor belongs
// to. Pagination under a changed query/sort/window/lang would silently skip
// or duplicate rows, so mismatched cursors are rejected instead.
func searchCursorScopeHash(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(strings.ToLower(strings.TrimSpace(part))))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func encodeSearchCursor(payload searchCursorPayload) (string, error) {
	if payload.Offset <= 0 && strings.TrimSpace(payload.ID) == "" {
		return "", nil
	}
	payload.V = searchCursorVersion
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

// searchCursorErrorMessage maps decode failures to a stable client message.
func searchCursorErrorMessage(err error) string {
	if errors.Is(err, errSearchCursorMismatch) {
		return errSearchCursorMismatch.Error()
	}
	return "cursor is malformed"
}

// lastEventKeyset extracts the (created_at, id) keyset position from the last
// event in a result page. Returns ok=false when the envelope cannot be read.
func lastEventKeyset(events []json.RawMessage) (int64, string, bool) {
	if len(events) == 0 {
		return 0, "", false
	}
	var envelope struct {
		ID        string `json:"id"`
		CreatedAt int64  `json:"created_at"`
	}
	if err := json.Unmarshal(events[len(events)-1], &envelope); err != nil {
		return 0, "", false
	}
	envelope.ID = strings.TrimSpace(envelope.ID)
	if envelope.ID == "" {
		return 0, "", false
	}
	return envelope.CreatedAt, envelope.ID, true
}

func decodeSearchCursor(value string, scopeHash string) (*searchCursorPayload, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}
	var payload searchCursorPayload
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, err
	}
	if payload.V != searchCursorVersion {
		return nil, errors.New("unsupported cursor version")
	}
	if payload.Offset < 0 {
		return nil, errors.New("cursor offset must be non-negative")
	}
	if payload.QHash != scopeHash {
		return nil, errSearchCursorMismatch
	}
	return &payload, nil
}
