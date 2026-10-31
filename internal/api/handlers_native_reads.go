package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

// GetEventActions returns one page of repost, reaction, or zap-receipt events
// that target the note. kind is required and must be 6, 7, or 9735. The items
// are those events, not author profiles.
func (h Handlers) GetEventActions(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	kindRaw := strings.TrimSpace(r.URL.Query().Get("kind"))
	if kindRaw == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "kind is required")
		return
	}
	kind, err := strconv.Atoi(kindRaw)
	if err != nil || (kind != 6 && kind != 7 && kind != 9735) {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "kind must be 6, 7, or 9735")
		return
	}
	h.writeEventActions(w, r, eventID, kind)
}

// GetEventZaps returns one page of kind 9735 zap receipts that target the note.
func (h Handlers) GetEventZaps(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	h.writeEventActions(w, r, eventID, 9735)
}

func (h Handlers) writeEventActions(w http.ResponseWriter, r *http.Request, eventID string, kind int) {
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
	page, err := h.service.GetEventActionsPage(r.Context(), eventID, kind, limit, cursor)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		if query.IsUnsupportedEventActionKind(err) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "kind must be 6, 7, or 9735")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "event actions are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	items := page.Events
	if items == nil {
		items = []json.RawMessage{}
	}
	nextCursor, err := encodeEventCursor(page.NextCursor)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := map[string]any{
		"event_id":    eventID,
		"kind":        kind,
		"items":       items,
		"consistency": "eventual",
	}
	if nextCursor != "" {
		payload["next_cursor"] = nextCursor
	}
	writeJSON(w, http.StatusOK, payload)
}

// GetMutualFollows returns pubkeys followed by both accounts. The same pubkey
// for the path and user_pubkey is that account's mutuals.
func (h Handlers) GetMutualFollows(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	userPubkey := normalizePathPubkey(r.URL.Query().Get("user_pubkey"))
	if userPubkey == "" {
		userPubkey = pubkey
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	after, err := decodePubkeyCursor(strings.TrimSpace(r.URL.Query().Get("cursor")))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
		return
	}
	pubkeys, next, err := h.service.GetMutualFollowsPage(r.Context(), pubkey, userPubkey, limit, after)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "mutual follows are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	if pubkeys == nil {
		pubkeys = []string{}
	}
	payload := map[string]any{
		"pubkey":      pubkey,
		"user_pubkey": userPubkey,
		"pubkeys":     pubkeys,
		"consistency": "eventual",
	}
	if next != "" {
		encoded, encErr := encodePubkeyCursor(next)
		if encErr != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		if encoded != "" {
			payload["next_cursor"] = encoded
		}
	}
	writeJSON(w, http.StatusOK, payload)
}

// GetNotifications returns one page of likes, reposts, quotes, zaps, replies,
// mentions, and new follows for pubkey.
func (h Handlers) GetNotifications(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cursor, err := decodeNotificationCursor(strings.TrimSpace(r.URL.Query().Get("cursor")))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
		return
	}
	items, next, err := h.service.ListEngagementNotifications(r.Context(), pubkey, limit, cursor)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "notifications are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	if items == nil {
		items = []query.EngagementNotification{}
	}
	payload := map[string]any{
		"pubkey":        pubkey,
		"notifications": items,
		"consistency":   "eventual",
	}
	if next != nil {
		encoded, encErr := encodeNotificationCursor(next)
		if encErr != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		if encoded != "" {
			payload["next_cursor"] = encoded
		}
	}
	writeJSON(w, http.StatusOK, payload)
}

// GetReplaceable returns the current parameterized replaceable event for
// pubkey, kind, and d-tag identifier, or 404 when the index has none.
func (h Handlers) GetReplaceable(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.URL.Query().Get("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	kindRaw := strings.TrimSpace(r.URL.Query().Get("kind"))
	kind, err := strconv.Atoi(kindRaw)
	if err != nil || kind <= 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "kind must be a positive integer")
		return
	}
	if _, ok := r.URL.Query()["identifier"]; !ok {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "identifier is required")
		return
	}
	identifier := r.URL.Query().Get("identifier")
	event, err := h.service.GetParameterizedReplaceableEvent(r.Context(), pubkey, kind, identifier)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "replaceable event not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "replaceable events are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event":       event,
		"consistency": "eventual",
	})
}

type pubkeyCursorPayload struct {
	Pubkey string `json:"pubkey"`
}

func encodePubkeyCursor(pubkey string) (string, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return "", nil
	}
	return encodeJSONCursor(pubkeyCursorPayload{Pubkey: pubkey})
}

func decodePubkeyCursor(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	var payload pubkeyCursorPayload
	if err := decodeJSONCursor(value, &payload); err != nil {
		return "", err
	}
	payload.Pubkey = strings.TrimSpace(payload.Pubkey)
	if payload.Pubkey == "" {
		return "", errCursorPayload
	}
	return payload.Pubkey, nil
}

type notificationCursorPayload struct {
	CreatedAt   int64  `json:"created_at"`
	Type        string `json:"type"`
	ActorPubkey string `json:"actor_pubkey"`
	EventID     string `json:"event_id"`
}

func encodeNotificationCursor(cursor *query.NotificationCursor) (string, error) {
	if cursor == nil || strings.TrimSpace(cursor.Type) == "" || strings.TrimSpace(cursor.ActorPubkey) == "" {
		return "", nil
	}
	return encodeJSONCursor(notificationCursorPayload{
		CreatedAt:   cursor.CreatedAt,
		Type:        cursor.Type,
		ActorPubkey: cursor.ActorPubkey,
		EventID:     cursor.EventID,
	})
}

func decodeNotificationCursor(value string) (*query.NotificationCursor, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	var payload notificationCursorPayload
	if err := decodeJSONCursor(value, &payload); err != nil {
		return nil, err
	}
	payload.Type = strings.TrimSpace(payload.Type)
	payload.ActorPubkey = strings.TrimSpace(payload.ActorPubkey)
	payload.EventID = strings.TrimSpace(payload.EventID)
	if payload.Type == "" || payload.ActorPubkey == "" {
		return nil, errCursorPayload
	}
	return &query.NotificationCursor{
		CreatedAt:   payload.CreatedAt,
		Type:        payload.Type,
		ActorPubkey: payload.ActorPubkey,
		EventID:     payload.EventID,
	}, nil
}

type followerCursorPayload struct {
	CreatedAt      int64  `json:"created_at"`
	SourceEventID  string `json:"source_event_id"`
	FollowerPubkey string `json:"follower_pubkey"`
}

func encodeFollowerCursor(cursor *query.FollowerCursor) (string, error) {
	if cursor == nil || strings.TrimSpace(cursor.SourceEventID) == "" || strings.TrimSpace(cursor.FollowerPubkey) == "" {
		return "", nil
	}
	return encodeJSONCursor(followerCursorPayload{
		CreatedAt:      cursor.ContactListCreatedAt,
		SourceEventID:  cursor.SourceEventID,
		FollowerPubkey: cursor.FollowerPubkey,
	})
}

func decodeFollowerCursor(value string) (*query.FollowerCursor, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	var payload followerCursorPayload
	if err := decodeJSONCursor(value, &payload); err != nil {
		return nil, err
	}
	payload.SourceEventID = strings.TrimSpace(payload.SourceEventID)
	payload.FollowerPubkey = strings.TrimSpace(payload.FollowerPubkey)
	if payload.SourceEventID == "" || payload.FollowerPubkey == "" {
		return nil, errCursorPayload
	}
	return &query.FollowerCursor{
		ContactListCreatedAt: payload.CreatedAt,
		SourceEventID:        payload.SourceEventID,
		FollowerPubkey:       payload.FollowerPubkey,
	}, nil
}

func encodeJSONCursor(payload any) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeJSONCursor(value string, dest any) error {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(decoded, dest)
}

var errCursorPayload = errString("cursor is malformed")

type errString string

func (e errString) Error() string { return string(e) }
