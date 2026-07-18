package api

import (
	"net/http"

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
	h.getKindScopedEvents(w, r, 30023, "long_form")
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
	items, err := h.service.GetFollowers(r.Context(), pubkey, limit)
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

// GetMuteList returns the muted identifiers (pubkeys, events, hashtags, words)
// from the pubkey's latest kind:10000 mute list.
func (h Handlers) GetMuteList(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	muted, err := h.service.GetMuteList(r.Context(), pubkey)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "mute lists are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":      pubkey,
		"mute_list":   muted,
		"consistency": "eventual",
	})
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
