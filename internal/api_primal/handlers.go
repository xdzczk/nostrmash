package api_primal

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

// EventReader accepts query-native or legacy store-backed readers.
type EventReader = any

// Handlers translates Primal-compatible requests/responses at the boundary only.
type Handlers struct {
	service      query.Service
	maxBatchSize int
}

type HandlersOptions struct {
	MaxBatchSize int
	QueryOptions query.ServiceOptions
}

func NewHandlers(reader EventReader, maxBatchSize int) Handlers {
	return NewHandlersWithOptions(reader, HandlersOptions{MaxBatchSize: maxBatchSize})
}

func NewHandlersWithOptions(reader EventReader, options HandlersOptions) Handlers {
	handlers, err := NewHandlersWithOptionsE(reader, options)
	if err != nil {
		panic(err)
	}
	return handlers
}

func NewHandlersWithOptionsE(reader EventReader, options HandlersOptions) (Handlers, error) {
	maxBatchSize := options.MaxBatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 200
	}
	service, err := query.NewServiceWithOptionsE(reader, options.QueryOptions)
	if err != nil {
		return Handlers{}, err
	}
	return Handlers{
		service:      service,
		maxBatchSize: maxBatchSize,
	}, nil
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

	raw, err := h.service.GetEvent(r.Context(), eventID)
	if err != nil {
		if query.IsNotFound(err) {
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
	if !decodeJSONBodyLimited(w, r, publicBatchBodyLimitBytes, &req) {
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

	foundByID, err := h.service.GetEvents(r.Context(), normalizedIDs)
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

	profile, err := h.service.GetProfile(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
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

type primalUserInfosRequest struct {
	Pubkeys []string `json:"pubkeys"`
}

type primalUserInfosResponse struct {
	Profiles       []primalProfileResponse `json:"profiles"`
	MissingPubkeys []string                `json:"missing_pubkeys"`
}

func (h Handlers) BatchGetUserInfos(w http.ResponseWriter, r *http.Request) {
	var req primalUserInfosRequest
	if !decodeJSONBodyLimited(w, r, publicBatchBodyLimitBytes, &req) {
		return
	}
	if len(req.Pubkeys) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkeys must not be empty")
		return
	}
	if len(req.Pubkeys) > h.maxBatchSize {
		writeError(r.Context(), w, http.StatusBadRequest, "batch_limit_exceeded", "requested pubkeys exceed maximum batch size")
		return
	}
	pubkeys := normalizeUniqueValues(req.Pubkeys)
	if len(pubkeys) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkeys must include at least one non-empty value")
		return
	}
	profiles, err := h.service.GetProfiles(r.Context(), pubkeys)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	resp := primalUserInfosResponse{
		Profiles:       make([]primalProfileResponse, 0, len(profiles.Profiles)),
		MissingPubkeys: append([]string(nil), profiles.MissingPubkeys...),
	}
	for _, profile := range profiles.Profiles {
		resp.Profiles = append(resp.Profiles, primalProfileResponse{
			Pubkey:            profile.Pubkey,
			MetadataEventID:   profile.MetadataEventID,
			MetadataCreatedAt: profile.MetadataCreatedAt,
			Profile:           profile.ProfileJSON,
		})
	}
	slices.Sort(resp.MissingPubkeys)
	writeJSON(w, http.StatusOK, resp)
}

func (h Handlers) GetThreadView(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("eventId"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	limit := parseBoundedPositiveInt(r.URL.Query().Get("limit"), 20, 100)
	maxDepth := parseBoundedPositiveInt(r.URL.Query().Get("max_depth"), 100, 100)
	cursor, err := decodeEventCursor(strings.TrimSpace(r.URL.Query().Get("cursor")))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
		return
	}
	thread, err := h.service.GetThread(r.Context(), query.ThreadRequest{
		EventID:  eventID,
		Limit:    limit,
		MaxDepth: maxDepth,
		Cursor:   cursor,
	})
	if err != nil {
		if errors.Is(err, query.ErrThreadEventNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	nextCursor, err := encodeEventCursor(thread.NextCursor)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, buildThreadViewResponse(
		eventID,
		thread.Event,
		thread.Ancestors,
		thread.MissingAncestorIDs,
		thread.Replies,
		nextCursor,
		thread.Consistency,
	))
}

func (h Handlers) GetAuthorEvents(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit := parseBoundedPositiveInt(r.URL.Query().Get("limit"), 20, 100)
	items, err := h.service.GetAuthorEvents(r.Context(), pubkey, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"pubkey": pubkey, "events": items})
}

func (h Handlers) GetAuthorReplies(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit := parseBoundedPositiveInt(r.URL.Query().Get("limit"), 20, 100)
	items, err := h.service.GetAuthorReplies(r.Context(), pubkey, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"pubkey": pubkey, "items": items})
}

func (h Handlers) GetEventActions(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	counts, err := h.service.GetEventActionCounts(r.Context(), eventID)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":       counts.EventID,
		"reply_count":    counts.ReplyCount,
		"reaction_count": counts.ReactionCount,
		"repost_count":   counts.RepostCount,
		"consistency":    counts.Consistency,
	})
}

func (h Handlers) GetContactList(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	entry, err := h.service.GetContactList(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "contact list not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":       entry.Pubkey,
		"event_id":     entry.EventID,
		"created_at":   entry.CreatedAt,
		"contacts":     entry.ContactsJSONRaw,
		"consistency":  "eventual",
		"projection_v": entry.DerivationVer,
	})
}

func (h Handlers) GetRelayList(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	entry, err := h.service.GetRelayList(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "relay list not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":       entry.Pubkey,
		"event_id":     entry.EventID,
		"created_at":   entry.CreatedAt,
		"relays":       entry.RelaysJSONRaw,
		"consistency":  "eventual",
		"projection_v": entry.DerivationVer,
	})
}
