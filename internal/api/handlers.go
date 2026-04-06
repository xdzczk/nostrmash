package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/failure"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
	"github.com/xdzczk/nostrmash/internal/transport/httpx"
)

// Health is a liveness probe: process is up; no dependency checks.
func Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready returns 200 when Postgres accepts a ping, else 503.
func Ready(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if pool == nil {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "dependency_unavailable", "database is not configured")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "dependency_unavailable", "database is not reachable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

type EventReader = query.Reader

type Handlers struct {
	store        EventReader
	service      query.Service
	maxBatchSize int
}

var apiErrLog = logging.New("api")

func NewHandlers(store EventReader, maxBatchSize int) Handlers {
	if maxBatchSize <= 0 {
		maxBatchSize = 200
	}
	return Handlers{
		store:        store,
		service:      query.NewService(store),
		maxBatchSize: maxBatchSize,
	}
}

func (h Handlers) GetEventByID(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	event, err := h.store.GetEventWithProvenance(r.Context(), eventID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
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

type profileResponse struct {
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
		if errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "profile not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, profileResponse{
		Pubkey:            profile.Pubkey,
		MetadataEventID:   profile.MetadataEventID,
		MetadataCreatedAt: profile.MetadataCreatedAt,
		Profile:           profile.ProfileJSON,
	})
}

type batchProfilesRequest struct {
	Pubkeys []string `json:"pubkeys"`
}

type batchProfilesResponse struct {
	Profiles       []profileResponse `json:"profiles"`
	MissingPubkeys []string          `json:"missing_pubkeys"`
}

func (h Handlers) BatchGetProfiles(w http.ResponseWriter, r *http.Request) {
	var req batchProfilesRequest
	if !decodeJSONBodyLimited(w, r, publicBatchBodyLimitBytes, &req, false) {
		return
	}
	if len(req.Pubkeys) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkeys must not be empty")
		return
	}
	if len(req.Pubkeys) > h.maxBatchSize {
		writeError(
			r.Context(),
			w,
			http.StatusBadRequest,
			"batch_limit_exceeded",
			"requested pubkeys exceed maximum batch size",
		)
		return
	}

	normalizedPubkeys := make([]string, 0, len(req.Pubkeys))
	seen := make(map[string]struct{}, len(req.Pubkeys))
	for _, pubkey := range req.Pubkeys {
		trimmed := strings.TrimSpace(pubkey)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalizedPubkeys = append(normalizedPubkeys, trimmed)
	}
	if len(normalizedPubkeys) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkeys must include at least one non-empty value")
		return
	}

	profiles, err := h.service.GetProfiles(r.Context(), normalizedPubkeys)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	resp := batchProfilesResponse{
		Profiles:       make([]profileResponse, 0, len(profiles.Profiles)),
		MissingPubkeys: append([]string(nil), profiles.MissingPubkeys...),
	}
	for _, profile := range profiles.Profiles {
		resp.Profiles = append(resp.Profiles, profileResponse{
			Pubkey:            profile.Pubkey,
			MetadataEventID:   profile.MetadataEventID,
			MetadataCreatedAt: profile.MetadataCreatedAt,
			Profile:           profile.ProfileJSON,
		})
	}
	slices.Sort(resp.MissingPubkeys)
	writeJSON(w, http.StatusOK, resp)
}

// GetAuthorEvents returns projected recent events sorted by created_at desc, id desc.
func (h Handlers) GetAuthorEvents(w http.ResponseWriter, r *http.Request) {
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
	events, err := h.service.GetAuthorEvents(r.Context(), pubkey, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey": pubkey,
		"events": events,
	})
}

// GetAuthorReplies returns replies authored by pubkey.
func (h Handlers) GetAuthorReplies(w http.ResponseWriter, r *http.Request) {
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
	replies, err := h.service.GetAuthorReplies(r.Context(), pubkey, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey": pubkey,
		"items":  replies,
	})
}

// GetEventCounts returns eventually-consistent interaction counters from Layer 3 projections.
func (h Handlers) GetEventCounts(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	counts, err := h.store.GetEventCounts(r.Context(), eventID)
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

// GetEventReplies returns direct replies ordered by created_at asc, id asc with cursor pagination.
func (h Handlers) GetEventReplies(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
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

	replies, next, err := h.store.GetEventReplies(r.Context(), eventID, limit, cursor)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	nextCursor, err := encodeEventCursor(next)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":    eventID,
		"replies":     replies,
		"next_cursor": nextCursor,
		"consistency": "eventual",
	})
}

// GetEventAncestors returns ancestors in root -> ... -> parent order.
func (h Handlers) GetEventAncestors(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	maxDepth, err := parseBoundedPositiveInt(r, "max_depth", 100, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	ancestors, missing, err := h.store.GetEventAncestors(r.Context(), eventID, maxDepth)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":             eventID,
		"ancestors":            ancestors,
		"missing_ancestor_ids": missing,
		"consistency":          "eventual",
	})
}

// GetThread returns a thread view for one event.
func (h Handlers) GetThread(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("eventId"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}

	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	maxDepth, err := parseBoundedPositiveInt(r, "max_depth", 100, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
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
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":             eventID,
		"event":                thread.Event,
		"ancestors":            thread.Ancestors,
		"missing_ancestor_ids": thread.MissingAncestorIDs,
		"replies":              thread.Replies,
		"next_cursor":          nextCursor,
		"consistency":          thread.Consistency,
	})
}

type relayHealthEntry struct {
	RelayURL           string     `json:"relay_url"`
	Mode               string     `json:"mode"`
	FilterGroup        string     `json:"filter_group"`
	Status             string     `json:"status"`
	LatestCheckpointAt time.Time  `json:"latest_checkpoint_at"`
	EOSESeenAt         *time.Time `json:"eose_seen_at,omitempty"`
}

// GetRelaysHealth returns an aggregate view of relay ingest state.
func (h Handlers) GetRelaysHealth(w http.ResponseWriter, r *http.Request) {
	rows, err := h.store.ListRelayHealth(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	relays := make([]relayHealthEntry, 0, len(rows))
	for _, row := range rows {
		relays = append(relays, relayHealthEntry{
			RelayURL:           row.RelayURL,
			Mode:               row.Mode,
			FilterGroup:        row.FilterGroup,
			Status:             row.Status,
			LatestCheckpointAt: row.UpdatedAt.UTC(),
			EOSESeenAt:         row.EOSESeenAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"relays":      relays,
		"consistency": "eventual",
	})
}

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

// Search returns a best-effort combined event/profile search.
func (h Handlers) Search(w http.ResponseWriter, r *http.Request) {
	queryText := strings.TrimSpace(r.URL.Query().Get("q"))
	if queryText == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "q is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	events, err := h.store.SearchEventsByContent(r.Context(), queryText, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	profiles, err := h.store.SearchProfiles(r.Context(), queryText, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	projectedProfiles := make([]profileResponse, 0, len(profiles))
	for _, profile := range profiles {
		projectedProfiles = append(projectedProfiles, profileResponse{
			Pubkey:            profile.Pubkey,
			MetadataEventID:   profile.MetadataEventID,
			MetadataCreatedAt: profile.MetadataCreatedAt,
			Profile:           profile.ProfileJSON,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"query":       queryText,
		"events":      events,
		"profiles":    projectedProfiles,
		"consistency": "eventual",
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

type apiErrorEnvelope struct {
	Error apiErrorBody `json:"error"`
}

type apiErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func writeError(ctx context.Context, w http.ResponseWriter, status int, code, message string) {
	class := failure.ClassifyHTTP(status, code)
	logging.WithRequestID(ctx, apiErrLog).Info("api_error_response",
		"failure_class", class.Class,
		"failure_reason", class.Reason,
		"status", status,
		"code", code,
		"request_id", logging.RequestIDFromContext(ctx),
		"trace_id", traceutil.TraceID(ctx),
	)
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

func parseBoundedPositiveInt(r *http.Request, key string, defaultValue int, maxValue int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, errors.New(key + " must be a positive integer")
	}
	if parsed > maxValue {
		return 0, errors.New(key + " exceeds maximum allowed value")
	}
	return parsed, nil
}

func encodeEventCursor(cursor *store.EventOrderCursor) (string, error) {
	if cursor == nil {
		return "", nil
	}
	return httpx.EncodeEventCursorPayload(httpx.EventCursorPayload{
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	})
}

func decodeEventCursor(value string) (*store.EventOrderCursor, error) {
	payload, err := httpx.DecodeEventCursorPayload(value)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return &store.EventOrderCursor{
		CreatedAt: payload.CreatedAt,
		ID:        payload.ID,
	}, nil
}
