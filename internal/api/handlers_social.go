package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/xdzczk/nostrmash/internal/store"
)

// GetContactList returns projected latest contact list (kind=3) for one pubkey.
func (h Handlers) GetContactList(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	contactList, err := h.service.GetContactList(r.Context(), pubkey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
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
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	relayList, err := h.service.GetRelayList(r.Context(), pubkey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
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
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	type replaceableReader interface {
		GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error)
	}
	if reader, ok := h.store.(replaceableReader); ok {
		event, err := reader.GetParameterizedReplaceableEvent(r.Context(), pubkey, 10003, "")
		if err == nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"pubkey":      pubkey,
				"bookmarks":   []json.RawMessage{event},
				"consistency": "eventual",
			})
			return
		}
		if !errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
	}
	h.getKindScopedEvents(w, r, 10003, "bookmarks")
}

func (h Handlers) GetHighlights(w http.ResponseWriter, r *http.Request) {
	h.getKindScopedEvents(w, r, 9802, "highlights")
}

func (h Handlers) GetLongForm(w http.ResponseWriter, r *http.Request) {
	h.getKindScopedEvents(w, r, 30023, "long_form")
}

func (h Handlers) GetZaps(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	type zapReader interface {
		GetUserZaps(ctx context.Context, pubkey string, limit int, sortBySats bool) ([]json.RawMessage, error)
	}
	if reader, ok := h.store.(zapReader); ok {
		zaps, err := reader.GetUserZaps(r.Context(), pubkey, limit, true)
		if err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"pubkey":      pubkey,
			"zaps":        zaps,
			"consistency": "eventual",
		})
		return
	}
	h.getKindScopedEvents(w, r, 9735, "zaps")
}

// GetMentions returns events referencing this pubkey via p-tags.
func (h Handlers) GetMentions(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	items, err := h.store.GetEventsReferencingPubkey(r.Context(), pubkey, limit)
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
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	items, err := h.store.GetFollowersByPubkey(r.Context(), pubkey, limit)
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

func (h Handlers) getKindScopedEvents(w http.ResponseWriter, r *http.Request, kind int, responseKey string) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
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
