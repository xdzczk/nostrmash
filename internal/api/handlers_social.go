package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

// GetContactList returns projected latest contact list (kind=3) for one pubkey.
func (h Handlers) GetContactList(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	contactList, err := h.service.GetContactList(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "contact list not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":       contactList.Pubkey,
		"event_id":     contactList.EventID,
		"created_at":   contactList.CreatedAt,
		"contacts":     contactList.ContactsJSONRaw,
		"consistency":  "eventual",
		"projection_v": contactList.DerivationVer,
	})
}

// GetRelayList returns projected latest relay list (kind=10002) for one pubkey.
func (h Handlers) GetRelayList(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	relayList, err := h.service.GetRelayList(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "relay list not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":       relayList.Pubkey,
		"event_id":     relayList.EventID,
		"created_at":   relayList.CreatedAt,
		"relays":       relayList.RelaysJSONRaw,
		"consistency":  "eventual",
		"projection_v": relayList.DerivationVer,
	})
}

func (h Handlers) GetBookmarks(w http.ResponseWriter, r *http.Request) {
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
	bookmarks, err := h.service.GetBookmarks(r.Context(), pubkey, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":      pubkey,
		"bookmarks":   bookmarks,
		"consistency": "eventual",
	})
}

func (h Handlers) GetHighlights(w http.ResponseWriter, r *http.Request) {
	h.getKindScopedEvents(w, r, 9802, "highlights")
}

func (h Handlers) GetLongForm(w http.ResponseWriter, r *http.Request) {
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
	cursor, err := decodeEventCursor(strings.TrimSpace(r.URL.Query().Get("cursor")))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
		return
	}
	result, err := h.service.GetLongFormPage(r.Context(), pubkey, limit, cursor)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "long-form pagination is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	items := result.Events
	if items == nil {
		items = []json.RawMessage{}
	}
	nextCursor, err := encodeEventCursor(result.NextCursor)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := map[string]any{
		"pubkey":      pubkey,
		"long_form":   items,
		"consistency": "eventual",
	}
	if nextCursor != "" {
		payload["next_cursor"] = nextCursor
	}
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetZaps(w http.ResponseWriter, r *http.Request) {
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
	zaps, err := h.service.GetUserZapsBySats(r.Context(), pubkey, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":      pubkey,
		"zaps":        zaps,
		"consistency": "eventual",
	})
}

// GetMentions returns events referencing this pubkey via p-tags.
func (h Handlers) GetMentions(w http.ResponseWriter, r *http.Request) {
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
	items, err := h.service.GetMentions(r.Context(), pubkey, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":      pubkey,
		"items":       items,
		"consistency": "eventual",
	})
}

// GetFollowers returns follower edges derived from latest contact lists.
func (h Handlers) GetFollowers(w http.ResponseWriter, r *http.Request) {
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
	cursor, err := decodeFollowerCursor(strings.TrimSpace(r.URL.Query().Get("cursor")))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
		return
	}
	items, next, err := h.service.GetFollowersPage(r.Context(), pubkey, limit, cursor)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "follower pagination is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	if items == nil {
		items = []json.RawMessage{}
	}
	nextCursor, err := encodeFollowerCursor(next)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := map[string]any{
		"pubkey":      pubkey,
		"items":       items,
		"consistency": "eventual",
	}
	if nextCursor != "" {
		payload["next_cursor"] = nextCursor
	}
	writeJSON(w, http.StatusOK, payload)
}

// GetMuteList returns the pubkey's latest kind 10000 mute list with each tag's
// name kept, plus the raw event when the index still has it. A missing list is
// an empty page, not a 404.
func (h Handlers) GetMuteList(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	detail, err := h.service.GetMuteListDetail(r.Context(), pubkey)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "mute lists are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	tags := detail.Tags
	if tags == nil {
		tags = []query.MuteListTag{}
	}
	values := make([]string, 0, len(tags))
	for _, tag := range tags {
		values = append(values, tag.Value)
	}
	payload := map[string]any{
		"pubkey":      pubkey,
		"mute_list":   values,
		"tags":        tags,
		"consistency": "eventual",
	}
	if len(detail.Event) > 0 {
		payload["event"] = detail.Event
	}
	writeJSON(w, http.StatusOK, payload)
}

// GetMutedBy returns authors who mute this pubkey (their latest kind:10000 mute
// list includes this pubkey as a p-tag).
func (h Handlers) GetMutedBy(w http.ResponseWriter, r *http.Request) {
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
	items, err := h.service.GetMutedBy(r.Context(), pubkey, limit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "muted-by lookups are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":      pubkey,
		"items":       items,
		"consistency": "eventual",
	})
}

func (h Handlers) getKindScopedEvents(w http.ResponseWriter, r *http.Request, kind int, responseKey string) {
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
	items, err := h.service.GetRecentEventsByKindAndPubkey(r.Context(), kind, pubkey, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":      pubkey,
		responseKey:   items,
		"consistency": "eventual",
	})
}

func (h Handlers) GetMedia(w http.ResponseWriter, r *http.Request) {
	h.writeAuthorEvents(w, r, h.service.GetAuthorMedia)
}

func (h Handlers) GetUserLive(w http.ResponseWriter, r *http.Request) {
	h.writeAuthorEvents(w, r, h.service.GetAuthorLive)
}

func (h Handlers) GetPins(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	items, err := h.service.GetAuthorPins(r.Context(), pubkey)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "pin lists are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":      pubkey,
		"items":       items,
		"consistency": "eventual",
	})
}

func (h Handlers) GetLive(w http.ResponseWriter, r *http.Request) {
	status := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))
	if status == "" {
		status = "live"
	}
	switch status {
	case "planned", "live", "ended":
	default:
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "status must be planned, live, or ended")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	items, err := h.service.GetLiveActivities(r.Context(), status, "", limit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "live activities are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":       items,
		"consistency": "eventual",
	})
}

func (h Handlers) GetEventComments(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("id")
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	items, err := h.service.GetEventComments(r.Context(), eventID, limit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "comments are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":    eventID,
		"items":       items,
		"consistency": "eventual",
	})
}

func (h Handlers) GetEventReports(w http.ResponseWriter, r *http.Request) {
	h.writeReports(w, r, r.PathValue("id"), true)
}

func (h Handlers) GetUserReports(w http.ResponseWriter, r *http.Request) {
	h.writeReports(w, r, normalizePathPubkey(r.PathValue("pubkey")), false)
}

func (h Handlers) writeAuthorEvents(
	w http.ResponseWriter,
	r *http.Request,
	load func(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error),
) {
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
	items, err := load(r.Context(), pubkey, limit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "this read is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":      pubkey,
		"items":       items,
		"consistency": "eventual",
	})
}

func (h Handlers) writeReports(w http.ResponseWriter, r *http.Request, target string, byEvent bool) {
	if target == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "target is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var items []query.ReportTarget
	if byEvent {
		items, err = h.service.GetEventReports(r.Context(), target, limit)
	} else {
		items, err = h.service.GetUserReports(r.Context(), target, limit)
	}
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "reports are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	body := map[string]any{
		"items":       items,
		"consistency": "eventual",
	}
	if byEvent {
		body["event_id"] = target
	} else {
		body["pubkey"] = target
	}
	writeJSON(w, http.StatusOK, body)
}
