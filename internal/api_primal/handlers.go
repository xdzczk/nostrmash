package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/store"
)

// EventReader is the narrow query surface required by the compatibility adapter.
type EventReader interface {
	GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error)
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
	GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error)
}

// Handlers translates Primal-compatible requests/responses at the boundary only.
type Handlers struct {
	store        EventReader
	maxBatchSize int
}

func NewHandlers(store EventReader, maxBatchSize int) Handlers {
	if maxBatchSize <= 0 {
		maxBatchSize = 200
	}
	return Handlers{store: store, maxBatchSize: maxBatchSize}
}

type primalEventResponse struct {
	Event json.RawMessage `json:"event"`
}

func (h Handlers) GetEventByID(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}

	raw, err := h.store.GetEventRawByID(r.Context(), eventID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, primalEventResponse{Event: raw})
}

type primalBatchEventsRequest struct {
	EventIDs []string `json:"event_ids"`

	// Compatibility quirk: accept native ids alias for transitional clients.
	IDs []string `json:"ids"`
}

func (r primalBatchEventsRequest) sourceIDs() []string {
	if len(r.EventIDs) > 0 {
		return r.EventIDs
	}
	return r.IDs
}

type primalBatchEventsResponse struct {
	Events          []json.RawMessage `json:"events"`
	MissingEventIDs []string          `json:"missing_event_ids"`
}

func (h Handlers) BatchGetEvents(w http.ResponseWriter, r *http.Request) {
	var req primalBatchEventsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return
	}
	inputIDs := req.sourceIDs()
	if len(inputIDs) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event_ids must not be empty")
		return
	}
	if len(inputIDs) > h.maxBatchSize {
		writeError(r.Context(), w, http.StatusBadRequest, "batch_limit_exceeded", "requested ids exceed maximum batch size")
		return
	}

	normalizedIDs := normalizeUniqueValues(inputIDs)
	if len(normalizedIDs) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event_ids must include at least one non-empty value")
		return
	}

	foundByID, err := h.store.GetEventRawsByIDs(r.Context(), normalizedIDs)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	resp := primalBatchEventsResponse{
		Events:          make([]json.RawMessage, 0, len(foundByID)),
		MissingEventIDs: make([]string, 0),
	}
	for _, id := range normalizedIDs {
		raw, ok := foundByID[id]
		if !ok {
			resp.MissingEventIDs = append(resp.MissingEventIDs, id)
			continue
		}
		resp.Events = append(resp.Events, raw)
	}
	slices.Sort(resp.MissingEventIDs)
	writeJSON(w, http.StatusOK, resp)
}

type primalProfileResponse struct {
	Pubkey            string          `json:"pubkey"`
	MetadataEventID   string          `json:"metadata_event_id"`
	MetadataCreatedAt int64           `json:"metadata_created_at"`
	Profile           json.RawMessage `json:"profile"`
}

func (h Handlers) GetProfileByPubkey(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}

	profile, err := h.store.GetProfileByPubkey(r.Context(), pubkey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "profile not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, primalProfileResponse{
		Pubkey:            profile.Pubkey,
		MetadataEventID:   profile.MetadataEventID,
		MetadataCreatedAt: profile.MetadataCreatedAt,
		Profile:           profile.ProfileJSON,
	})
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
