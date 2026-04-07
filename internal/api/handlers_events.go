package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
)

func (h Handlers) GetEventByID(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	event, err := h.store.GetEventWithProvenance(r.Context(), eventID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		raw, fallbackErr := h.service.GetEvent(r.Context(), eventID)
		if fallbackErr != nil {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		var payload any
		if unmarshalErr := json.Unmarshal(raw, &payload); unmarshalErr != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "stored event payload is invalid")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"event": payload,
			"provenance": map[string]any{
				"relays": []seenOnEntry{},
			},
			"consistency": "eventual",
		})
		return
	}
	var payload any
	if err := json.Unmarshal(event.Event, &payload); err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "stored event payload is invalid")
		return
	}
	relays := make([]seenOnEntry, 0, len(event.Relays))
	for _, relay := range event.Relays {
		relays = append(relays, seenOnEntry{
			RelayURL: relay.RelayURL,
			SeenAt:   relay.SeenAt.UTC(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event": payload,
		"provenance": map[string]any{
			"relays": relays,
		},
		"consistency": "strong",
	})
}

type batchEventsRequest struct {
	IDs []string `json:"ids"`
}

type batchEventsResponse struct {
	Events  []json.RawMessage `json:"events"`
	Missing []string          `json:"missing"`
}

func (h Handlers) BatchGetEvents(w http.ResponseWriter, r *http.Request) {
	var req batchEventsRequest
	if !decodeJSONBodyLimited(w, r, publicBatchBodyLimitBytes, &req, false) {
		return
	}

	if len(req.IDs) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "ids must not be empty")
		return
	}
	if len(req.IDs) > h.maxBatchSize {
		writeError(
			r.Context(),
			w,
			http.StatusBadRequest,
			"batch_limit_exceeded",
			"requested ids exceed maximum batch size",
		)
		return
	}

	normalizedIDs := make([]string, 0, len(req.IDs))
	seen := make(map[string]struct{}, len(req.IDs))
	for _, id := range req.IDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalizedIDs = append(normalizedIDs, trimmed)
	}
	if len(normalizedIDs) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "ids must include at least one non-empty value")
		return
	}

	foundByID, err := h.service.GetEvents(r.Context(), normalizedIDs)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	resp := batchEventsResponse{
		Events:  make([]json.RawMessage, 0, len(foundByID)),
		Missing: make([]string, 0),
	}
	for _, id := range normalizedIDs {
		raw, ok := foundByID[id]
		if !ok {
			resp.Missing = append(resp.Missing, id)
			continue
		}
		resp.Events = append(resp.Events, raw)
	}
	slices.Sort(resp.Missing)
	writeJSON(w, http.StatusOK, resp)
}

type seenOnEntry struct {
	RelayURL string    `json:"relay_url"`
	SeenAt   time.Time `json:"seen_at"`
}

type seenOnResponse struct {
	EventID string        `json:"event_id"`
	SeenOn  []seenOnEntry `json:"seen_on"`
}

func (h Handlers) GetEventSeenOn(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	seenOn, err := h.store.GetEventSeenOn(r.Context(), eventID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	resp := seenOnResponse{
		EventID: eventID,
		SeenOn:  make([]seenOnEntry, 0, len(seenOn)),
	}
	for _, relay := range seenOn {
		resp.SeenOn = append(resp.SeenOn, seenOnEntry{
			RelayURL: relay.RelayURL,
			SeenAt:   relay.SeenAt.UTC(),
		})
	}
	writeJSON(w, http.StatusOK, resp)
}
