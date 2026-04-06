package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
)

type AdminService interface {
	GetRelays(context.Context) ([]adminRelayState, error)
	GetJobs(context.Context, int) (adminJobsResponse, error)
	GetInvalidEvents(context.Context, int) (adminInvalidEventsResponse, error)
	GetRebuilds(context.Context, int) ([]adminRebuildRunResponse, error)
	TriggerRebuild(context.Context, derivation.TriggerProjectionRebuildParams) (adminRebuildRunResponse, error)
	GetStorage(context.Context) (adminStorageResponse, error)
	GetSystem(context.Context) (adminSystemResponse, error)
	GetDerivationVersions(context.Context) ([]adminDerivationVersionResponse, error)
}

type AdminServiceOptions struct {
	ServiceName      string
	Environment      string
	AppVersion       string
	StartedAt        time.Time
	ConfiguredRelays []string
	DisabledRelays   []string
}

type adminService struct {
	pool       *pgxpool.Pool
	derivation *derivation.Handlers

	serviceName string
	environment string
	appVersion  string
	startedAt   time.Time

	configuredRelays []string
	disabledRelays   map[string]struct{}
}

func NewAdminService(
	pool *pgxpool.Pool,
	derivationHandlers *derivation.Handlers,
	opts AdminServiceOptions,
) AdminService {
	disabled := make(map[string]struct{}, len(opts.DisabledRelays))
	for _, relayURL := range opts.DisabledRelays {
		trimmed := strings.TrimSpace(relayURL)
		if trimmed == "" {
			continue
		}
		disabled[trimmed] = struct{}{}
	}
	startedAt := opts.StartedAt.UTC()
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	return &adminService{
		pool:             pool,
		derivation:       derivationHandlers,
		serviceName:      strings.TrimSpace(opts.ServiceName),
		environment:      strings.TrimSpace(opts.Environment),
		appVersion:       strings.TrimSpace(opts.AppVersion),
		startedAt:        startedAt,
		configuredRelays: append([]string(nil), opts.ConfiguredRelays...),
		disabledRelays:   disabled,
	}
}

type AdminHandlers struct {
	service AdminService
}

func NewAdminHandlers(service AdminService) AdminHandlers {
	return AdminHandlers{service: service}
}

func (h AdminHandlers) GetRelays(w http.ResponseWriter, r *http.Request) {
	relays, err := h.service.GetRelays(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"relays": relays})
}

func (h AdminHandlers) GetJobs(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 500)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	resp, err := h.service.GetJobs(r.Context(), limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h AdminHandlers) GetInvalidEvents(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 500)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	resp, err := h.service.GetInvalidEvents(r.Context(), limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h AdminHandlers) GetRebuilds(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 500)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	runs, err := h.service.GetRebuilds(r.Context(), limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rebuilds": runs})
}

func (h AdminHandlers) TriggerRebuild(w http.ResponseWriter, r *http.Request) {
	var req triggerRebuildRequest
	if !decodeJSONBodyLimited(w, r, adminBodyLimitBytes, &req, true) {
		return
	}

	params := derivation.TriggerProjectionRebuildParams{
		DerivationName: strings.TrimSpace(req.DerivationName),
		TargetVersion:  req.TargetVersion,
		Scope: derivation.ProjectionRebuildScope{
			Type:           strings.TrimSpace(req.Scope.Type),
			EventID:        strings.TrimSpace(req.Scope.EventID),
			Pubkey:         strings.TrimSpace(req.Scope.Pubkey),
			StartCreatedAt: req.Scope.StartCreatedAt,
			EndCreatedAt:   req.Scope.EndCreatedAt,
		},
	}
	run, err := h.service.TriggerRebuild(r.Context(), params)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

func (h AdminHandlers) GetStorage(w http.ResponseWriter, r *http.Request) {
	resp, err := h.service.GetStorage(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h AdminHandlers) GetSystem(w http.ResponseWriter, r *http.Request) {
	resp, err := h.service.GetSystem(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h AdminHandlers) GetDerivationVersions(w http.ResponseWriter, r *http.Request) {
	versions, err := h.service.GetDerivationVersions(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"versions": versions})
}

type triggerRebuildRequest struct {
	DerivationName string                    `json:"derivation_name"`
	TargetVersion  int                       `json:"target_version"`
	Scope          triggerRebuildScopeDetail `json:"scope"`
}

type triggerRebuildScopeDetail struct {
	Type           string `json:"type"`
	EventID        string `json:"event_id"`
	Pubkey         string `json:"pubkey"`
	StartCreatedAt *int64 `json:"start_created_at"`
	EndCreatedAt   *int64 `json:"end_created_at"`
}
