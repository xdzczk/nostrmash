package api

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/meili"
	"github.com/xdzczk/nostrmash/internal/trust"
)

type AdminService interface {
	GetRelays(context.Context) ([]adminRelayState, error)
	GetRelaySuggestions(context.Context, int, bool) ([]adminRelaySuggestion, error)
	GetJobs(context.Context, int) (adminJobsResponse, error)
	GetInvalidEvents(context.Context, int) (adminInvalidEventsResponse, error)
	GetProjectionStatus(context.Context) (adminProjectionStatusResponse, error)
	GetDiscoveryStatus(context.Context) (adminDiscoveryStatusResponse, error)
	GetSearchStatus(context.Context) (adminSearchStatusResponse, error)
	TriggerMeilisearchSync(context.Context, int) (adminMeilisearchSyncResponse, error)
	GetRebuilds(context.Context, int) ([]adminRebuildRunResponse, error)
	TriggerRebuild(context.Context, derivation.TriggerProjectionRebuildParams) (adminRebuildRunResponse, error)
	GetStorage(context.Context) (adminStorageResponse, error)
	GetStorageIndexes(context.Context) (adminStorageIndexesResponse, error)
	GetSystem(context.Context) (adminSystemResponse, error)
	GetDerivationVersions(context.Context) ([]adminDerivationVersionResponse, error)
	GetTrustRuns(context.Context, int) ([]adminTrustRunResponse, error)
	GetTrustRun(context.Context, int64) (adminTrustRunResponse, error)
	TriggerTrustRun(context.Context) (adminTrustRunResponse, error)
	GetTopTrustScores(context.Context, int) ([]adminTrustScoreResponse, error)

	GetRelayRegistry(context.Context, int) (adminRelayRegistryResponse, error)
	GetRelayRegistryDesired(context.Context) (adminRelayRegistryDesiredResponse, error)
	SetRelayRegistryPolicy(context.Context, adminSetPolicyRequest) error
	GetRelayDiagnostics(context.Context, string) (adminRelayDiagnosticsResponse, error)
	GetRelayAdmissionDryRun(context.Context) (adminAdmissionDryRunResponse, error)

	SetAccountState(ctx context.Context, pubkey, state, reason string) (adminAccountStateResponse, error)
	EnqueueAccountHydration(ctx context.Context, pubkey, reason string) (accountHydrateResponse, error)
}

type AdminServiceOptions struct {
	ServiceName          string
	Environment          string
	AppVersion           string
	StartedAt            time.Time
	ConfiguredRelays     []string
	DisabledRelays       []string
	DiscoveryTrustMode   string
	SearchTrustMode      string
	TrustRefreshInterval time.Duration
	MeiliClient          *meili.Client
	AdmissionConfig      config.RelayRegistryAdmissionConfig
	Hydration            config.HydrationConfig
}

type adminService struct {
	pool       *pgxpool.Pool
	derivation *derivation.Handlers
	trust      *trust.Runtime

	serviceName string
	environment string
	appVersion  string
	startedAt   time.Time

	configuredRelays     []string
	disabledRelays       map[string]struct{}
	discoveryTrustMode   string
	searchTrustMode      string
	trustRefreshInterval time.Duration
	meili                *meili.Client
	admissionCfg         config.RelayRegistryAdmissionConfig
	hydration            config.HydrationConfig
}

func NewAdminService(
	pool *pgxpool.Pool,
	derivationHandlers *derivation.Handlers,
	trustRuntime *trust.Runtime,
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
		pool:                 pool,
		derivation:           derivationHandlers,
		trust:                trustRuntime,
		serviceName:          strings.TrimSpace(opts.ServiceName),
		environment:          strings.TrimSpace(opts.Environment),
		appVersion:           strings.TrimSpace(opts.AppVersion),
		startedAt:            startedAt,
		configuredRelays:     append([]string(nil), opts.ConfiguredRelays...),
		disabledRelays:       disabled,
		discoveryTrustMode:   strings.ToLower(strings.TrimSpace(opts.DiscoveryTrustMode)),
		searchTrustMode:      strings.ToLower(strings.TrimSpace(opts.SearchTrustMode)),
		trustRefreshInterval: opts.TrustRefreshInterval,
		meili:                opts.MeiliClient,
		admissionCfg:         opts.AdmissionConfig,
		hydration:            opts.Hydration,
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

func (h AdminHandlers) GetRelaySuggestions(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 500)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	recommendedOnly := false
	rawRecommendedOnly := strings.TrimSpace(r.URL.Query().Get("recommended_only"))
	if rawRecommendedOnly != "" {
		v, err := strconv.ParseBool(rawRecommendedOnly)
		if err != nil {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "recommended_only must be a boolean")
			return
		}
		recommendedOnly = v
	}
	suggestions, err := h.service.GetRelaySuggestions(r.Context(), limit, recommendedOnly)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"suggestions": suggestions})
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

func (h AdminHandlers) GetProjectionStatus(w http.ResponseWriter, r *http.Request) {
	resp, err := h.service.GetProjectionStatus(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h AdminHandlers) GetDiscoveryStatus(w http.ResponseWriter, r *http.Request) {
	resp, err := h.service.GetDiscoveryStatus(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h AdminHandlers) GetSearchStatus(w http.ResponseWriter, r *http.Request) {
	resp, err := h.service.GetSearchStatus(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h AdminHandlers) TriggerMeilisearchSync(w http.ResponseWriter, r *http.Request) {
	batchSize, err := parseBoundedPositiveInt(r, "batch_size", 1000, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	wait := false
	if rawWait := strings.TrimSpace(r.URL.Query().Get("wait")); rawWait != "" {
		parsedWait, parseErr := strconv.ParseBool(rawWait)
		if parseErr != nil {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "wait must be a boolean")
			return
		}
		wait = parsedWait
	}
	if !wait {
		startedAt := time.Now().UTC()
		go func(batch int) {
			// Full corpus sync streams notes + profiles + search_documents and
			// can run for several hours on a large production index. 30m was
			// cancelling mid-stream and leaving Meilisearch only partially filled.
			bgCtx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
			defer cancel()
			if _, syncErr := h.service.TriggerMeilisearchSync(bgCtx, batch); syncErr != nil {
				log.Printf("admin_meilisearch_sync_async_failed: batch_size=%d err=%v", batch, syncErr)
			}
		}(batchSize)
		writeJSON(w, http.StatusAccepted, adminMeilisearchSyncResponse{
			StartedAt: startedAt,
			BatchSize: batchSize,
			Async:     true,
			Status:    "started",
		})
		return
	}
	resp, err := h.service.TriggerMeilisearchSync(r.Context(), batchSize)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusAccepted, resp)
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

func (h AdminHandlers) GetStorageIndexes(w http.ResponseWriter, r *http.Request) {
	resp, err := h.service.GetStorageIndexes(r.Context())
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

func (h AdminHandlers) GetTrustRuns(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 500)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	runs, err := h.service.GetTrustRuns(r.Context(), limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"trust_runs": runs})
}

func (h AdminHandlers) GetTrustRun(w http.ResponseWriter, r *http.Request) {
	runID, err := parsePathInt64(r, "runID")
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	run, err := h.service.GetTrustRun(r.Context(), runID)
	if err != nil {
		writeError(r.Context(), w, http.StatusNotFound, "not_found", "trust run not found")
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (h AdminHandlers) TriggerTrustRun(w http.ResponseWriter, r *http.Request) {
	run, err := h.service.TriggerTrustRun(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

func (h AdminHandlers) GetTopTrustScores(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 500)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	scores, err := h.service.GetTopTrustScores(r.Context(), limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scores": scores})
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

func parsePathInt64(r *http.Request, key string) (int64, error) {
	raw := strings.TrimSpace(r.PathValue(key))
	if raw == "" {
		return 0, strconv.ErrSyntax
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return 0, strconv.ErrSyntax
	}
	return v, nil
}
