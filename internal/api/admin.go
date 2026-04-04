package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
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
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
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

type adminRelayState struct {
	RelayURL           string                 `json:"relay_url"`
	Configured         bool                   `json:"configured"`
	Disabled           bool                   `json:"disabled"`
	LatestCheckpointAt *time.Time             `json:"latest_checkpoint_at,omitempty"`
	Checkpoints        []adminRelayCheckpoint `json:"checkpoints"`
}

type adminRelayCheckpoint struct {
	Mode        string     `json:"mode"`
	FilterGroup string     `json:"filter_group"`
	Status      string     `json:"status"`
	Since       *int64     `json:"since,omitempty"`
	Until       *int64     `json:"until,omitempty"`
	Cursor      *string    `json:"cursor,omitempty"`
	EOSESeenAt  *time.Time `json:"eose_seen_at,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func (s *adminService) GetRelays(ctx context.Context) ([]adminRelayState, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT relay_url, mode, filter_group, "since", "until", cursor, eose_seen_at, status, updated_at
		FROM ingest_checkpoints
		ORDER BY relay_url ASC, mode ASC, filter_group ASC
	`)
	if err != nil {
		if strings.Contains(err.Error(), `column "since" does not exist`) {
			rows, err = s.pool.Query(ctx, `
				SELECT relay_url, mode, filter_group, since_ts, until_ts, cursor_val, eose_seen_at, status, updated_at
				FROM ingest_checkpoints
				ORDER BY relay_url ASC, mode ASC, filter_group ASC
			`)
		}
		if err != nil {
			return nil, fmt.Errorf("list relay checkpoints: %w", err)
		}
	}
	defer rows.Close()

	relayByURL := make(map[string]*adminRelayState)
	for _, relayURL := range s.configuredRelays {
		trimmed := strings.TrimSpace(relayURL)
		if trimmed == "" {
			continue
		}
		relayByURL[trimmed] = &adminRelayState{
			RelayURL:    trimmed,
			Configured:  true,
			Disabled:    s.isDisabledRelay(trimmed),
			Checkpoints: make([]adminRelayCheckpoint, 0),
		}
	}

	for rows.Next() {
		var checkpoint model.IngestCheckpoint
		if err := rows.Scan(
			&checkpoint.RelayURL,
			&checkpoint.Mode,
			&checkpoint.FilterGroup,
			&checkpoint.Since,
			&checkpoint.Until,
			&checkpoint.Cursor,
			&checkpoint.EOSESeenAt,
			&checkpoint.Status,
			&checkpoint.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan relay checkpoint: %w", err)
		}
		relayURL := strings.TrimSpace(checkpoint.RelayURL)
		entry, ok := relayByURL[relayURL]
		if !ok {
			entry = &adminRelayState{
				RelayURL:    relayURL,
				Checkpoints: make([]adminRelayCheckpoint, 0, 1),
			}
			relayByURL[relayURL] = entry
		}
		entry.Checkpoints = append(entry.Checkpoints, adminRelayCheckpoint{
			Mode:        checkpoint.Mode,
			FilterGroup: checkpoint.FilterGroup,
			Status:      checkpoint.Status,
			Since:       checkpoint.Since,
			Until:       checkpoint.Until,
			Cursor:      checkpoint.Cursor,
			EOSESeenAt:  checkpoint.EOSESeenAt,
			UpdatedAt:   checkpoint.UpdatedAt.UTC(),
		})
		if entry.LatestCheckpointAt == nil || checkpoint.UpdatedAt.After(*entry.LatestCheckpointAt) {
			latest := checkpoint.UpdatedAt.UTC()
			entry.LatestCheckpointAt = &latest
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read relay checkpoints: %w", err)
	}

	relays := make([]adminRelayState, 0, len(relayByURL))
	for _, relayState := range relayByURL {
		slices.SortFunc(relayState.Checkpoints, func(a, b adminRelayCheckpoint) int {
			if cmp := strings.Compare(a.Mode, b.Mode); cmp != 0 {
				return cmp
			}
			return strings.Compare(a.FilterGroup, b.FilterGroup)
		})
		relays = append(relays, *relayState)
	}
	slices.SortFunc(relays, func(a, b adminRelayState) int {
		return strings.Compare(a.RelayURL, b.RelayURL)
	})
	return relays, nil
}

func (s *adminService) isDisabledRelay(relayURL string) bool {
	_, ok := s.disabledRelays[relayURL]
	return ok
}

type adminJobsResponse struct {
	Counts         map[string]int64 `json:"counts"`
	DueNow         int64            `json:"due_now"`
	OldestPendingS int64            `json:"oldest_pending_s"`
	Recent         []adminJobItem   `json:"recent"`
}

type adminJobItem struct {
	ID          int64           `json:"id"`
	JobType     string          `json:"job_type"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAfter    time.Time       `json:"run_after"`
	LockedBy    *string         `json:"locked_by,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func (s *adminService) GetJobs(ctx context.Context, limit int) (adminJobsResponse, error) {
	resp := adminJobsResponse{
		Counts: map[string]int64{
			"pending":   0,
			"running":   0,
			"succeeded": 0,
			"dead":      0,
		},
		Recent: make([]adminJobItem, 0),
	}
	countRows, err := s.pool.Query(ctx, `
		SELECT status, COUNT(*)
		FROM jobs
		GROUP BY status
	`)
	if err != nil {
		return resp, fmt.Errorf("count jobs by status: %w", err)
	}
	for countRows.Next() {
		var status string
		var count int64
		if err := countRows.Scan(&status, &count); err != nil {
			countRows.Close()
			return resp, fmt.Errorf("scan jobs count row: %w", err)
		}
		resp.Counts[strings.TrimSpace(status)] = count
	}
	if err := countRows.Err(); err != nil {
		countRows.Close()
		return resp, fmt.Errorf("read jobs count rows: %w", err)
	}
	countRows.Close()

	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM jobs
		WHERE status = 'pending' AND run_after <= now()
	`).Scan(&resp.DueNow); err != nil {
		return resp, fmt.Errorf("count due jobs: %w", err)
	}

	var oldestPendingSeconds *float64
	if err := s.pool.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM (now() - MIN(run_after)))
		FROM jobs
		WHERE status = 'pending'
	`).Scan(&oldestPendingSeconds); err != nil {
		return resp, fmt.Errorf("get oldest pending age: %w", err)
	}
	if oldestPendingSeconds != nil && *oldestPendingSeconds > 0 {
		resp.OldestPendingS = int64(*oldestPendingSeconds)
	}

	recentRows, err := s.pool.Query(ctx, `
		SELECT id, job_type, status, attempts, max_attempts, run_after, locked_by, last_error, payload, created_at, updated_at
		FROM jobs
		ORDER BY created_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return resp, fmt.Errorf("list recent jobs: %w", err)
	}
	defer recentRows.Close()
	for recentRows.Next() {
		var item adminJobItem
		var payloadBytes []byte
		if err := recentRows.Scan(
			&item.ID,
			&item.JobType,
			&item.Status,
			&item.Attempts,
			&item.MaxAttempts,
			&item.RunAfter,
			&item.LockedBy,
			&item.LastError,
			&payloadBytes,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return resp, fmt.Errorf("scan recent job: %w", err)
		}
		item.Payload = append(json.RawMessage(nil), payloadBytes...)
		item.RunAfter = item.RunAfter.UTC()
		item.CreatedAt = item.CreatedAt.UTC()
		item.UpdatedAt = item.UpdatedAt.UTC()
		resp.Recent = append(resp.Recent, item)
	}
	if err := recentRows.Err(); err != nil {
		return resp, fmt.Errorf("read recent jobs: %w", err)
	}
	return resp, nil
}

type adminInvalidEventsResponse struct {
	Total       int64                 `json:"total"`
	Last24Hours int64                 `json:"last_24h"`
	ByErrorCode map[string]int64      `json:"by_error_code"`
	Items       []adminInvalidEvent   `json:"items"`
}

type adminInvalidEvent struct {
	ID           int64      `json:"id"`
	SourceRelay  *string    `json:"source_relay,omitempty"`
	ErrorCode    string     `json:"error_code"`
	ErrorMessage string     `json:"error_message"`
	RawPayload   *json.RawMessage `json:"raw_payload,omitempty"`
	SeenAt       time.Time  `json:"seen_at"`
}

func (s *adminService) GetInvalidEvents(ctx context.Context, limit int) (adminInvalidEventsResponse, error) {
	resp := adminInvalidEventsResponse{
		ByErrorCode: make(map[string]int64),
		Items:       make([]adminInvalidEvent, 0),
	}
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM invalid_events`).Scan(&resp.Total); err != nil {
		return resp, fmt.Errorf("count invalid events: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM invalid_events
		WHERE seen_at >= now() - interval '24 hours'
	`).Scan(&resp.Last24Hours); err != nil {
		return resp, fmt.Errorf("count invalid events last 24h: %w", err)
	}

	byCodeRows, err := s.pool.Query(ctx, `
		SELECT error_code, COUNT(*)
		FROM invalid_events
		GROUP BY error_code
		ORDER BY COUNT(*) DESC, error_code ASC
	`)
	if err != nil {
		return resp, fmt.Errorf("group invalid events by code: %w", err)
	}
	for byCodeRows.Next() {
		var code string
		var count int64
		if err := byCodeRows.Scan(&code, &count); err != nil {
			byCodeRows.Close()
			return resp, fmt.Errorf("scan invalid events by code: %w", err)
		}
		resp.ByErrorCode[code] = count
	}
	if err := byCodeRows.Err(); err != nil {
		byCodeRows.Close()
		return resp, fmt.Errorf("read invalid events by code: %w", err)
	}
	byCodeRows.Close()

	itemsRows, err := s.pool.Query(ctx, `
		SELECT id, source_relay, error_code, error_message, raw_payload, seen_at
		FROM invalid_events
		ORDER BY seen_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return resp, fmt.Errorf("list invalid events: %w", err)
	}
	defer itemsRows.Close()
	for itemsRows.Next() {
		var item adminInvalidEvent
		var payloadBytes []byte
		if err := itemsRows.Scan(
			&item.ID,
			&item.SourceRelay,
			&item.ErrorCode,
			&item.ErrorMessage,
			&payloadBytes,
			&item.SeenAt,
		); err != nil {
			return resp, fmt.Errorf("scan invalid event row: %w", err)
		}
		item.SeenAt = item.SeenAt.UTC()
		if len(payloadBytes) > 0 {
			payload := json.RawMessage(append([]byte(nil), payloadBytes...))
			item.RawPayload = &payload
		}
		resp.Items = append(resp.Items, item)
	}
	if err := itemsRows.Err(); err != nil {
		return resp, fmt.Errorf("read invalid event rows: %w", err)
	}
	return resp, nil
}

type adminRebuildRunResponse struct {
	ID             int64                  `json:"id"`
	DerivationName string                 `json:"derivation_name"`
	TargetVersion  int                    `json:"target_version"`
	Scope          derivation.ProjectionRebuildScope `json:"scope"`
	Status         string                 `json:"status"`
	JobID          *int64                 `json:"job_id,omitempty"`
	Attempts       int                    `json:"attempts"`
	StartedAt      *time.Time             `json:"started_at,omitempty"`
	FinishedAt     *time.Time             `json:"finished_at,omitempty"`
	LastError      *string                `json:"last_error,omitempty"`
}

func (s *adminService) GetRebuilds(ctx context.Context, limit int) ([]adminRebuildRunResponse, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id,
			derivation_name,
			target_version,
			scope_type,
			scope_event_id,
			scope_pubkey,
			scope_start_created_at,
			scope_end_created_at,
			status,
			job_id,
			attempts,
			started_at,
			finished_at,
			last_error
		FROM projection_rebuild_runs
		ORDER BY created_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list rebuild runs: %w", err)
	}
	defer rows.Close()
	out := make([]adminRebuildRunResponse, 0, limit)
	for rows.Next() {
		run, err := scanAdminRebuildRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rebuild rows: %w", err)
	}
	return out, nil
}

func (s *adminService) TriggerRebuild(ctx context.Context, params derivation.TriggerProjectionRebuildParams) (adminRebuildRunResponse, error) {
	run, err := s.derivation.TriggerProjectionRebuild(ctx, params)
	if err != nil {
		return adminRebuildRunResponse{}, err
	}
	return asAdminRebuildRun(run), nil
}

type adminStorageResponse struct {
	DatabaseBytes int64                      `json:"database_bytes"`
	Tables        []adminStorageTableDetails `json:"tables"`
}

type adminStorageTableDetails struct {
	TableName  string `json:"table_name"`
	RowCount   int64  `json:"row_count"`
	StorageB   int64  `json:"storage_bytes"`
}

var trackedStorageTables = []string{
	"events",
	"event_tags",
	"event_relays",
	"invalid_events",
	"jobs",
	"derivation_active_versions",
	"projection_rebuild_runs",
	"profiles_latest",
	"author_recent_events",
	"thread_edges",
}

func (s *adminService) GetStorage(ctx context.Context) (adminStorageResponse, error) {
	resp := adminStorageResponse{
		Tables: make([]adminStorageTableDetails, 0, len(trackedStorageTables)),
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT pg_database_size(current_database())
	`).Scan(&resp.DatabaseBytes); err != nil {
		return resp, fmt.Errorf("get database size: %w", err)
	}
	for _, tableName := range trackedStorageTables {
		var rowCount int64
		if err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName)).Scan(&rowCount); err != nil {
			return resp, fmt.Errorf("count table %s: %w", tableName, err)
		}
		var tableBytes *int64
		if err := s.pool.QueryRow(ctx, `
			SELECT pg_total_relation_size(to_regclass($1))
		`, tableName).Scan(&tableBytes); err != nil {
			return resp, fmt.Errorf("size table %s: %w", tableName, err)
		}
		storageBytes := int64(0)
		if tableBytes != nil {
			storageBytes = *tableBytes
		}
		resp.Tables = append(resp.Tables, adminStorageTableDetails{
			TableName: tableName,
			RowCount:  rowCount,
			StorageB:  storageBytes,
		})
	}
	return resp, nil
}

type adminSystemResponse struct {
	ServiceName string              `json:"service_name"`
	Environment string              `json:"environment"`
	AppVersion  string              `json:"app_version"`
	NowUTC      time.Time           `json:"now_utc"`
	UptimeS     int64               `json:"uptime_s"`
	Runtime     adminRuntimeDetails `json:"runtime"`
	Database    adminDatabaseStatus `json:"database"`
}

type adminRuntimeDetails struct {
	GoVersion    string `json:"go_version"`
	NumGoroutine int    `json:"num_goroutine"`
}

type adminDatabaseStatus struct {
	Reachable    bool   `json:"reachable"`
	PingMS       int64  `json:"ping_ms"`
	MaxConns     int32  `json:"max_conns"`
	TotalConns   int32  `json:"total_conns"`
	IdleConns    int32  `json:"idle_conns"`
	AcquiredConns int32 `json:"acquired_conns"`
}

func (s *adminService) GetSystem(ctx context.Context) (adminSystemResponse, error) {
	now := time.Now().UTC()
	resp := adminSystemResponse{
		ServiceName: s.serviceName,
		Environment: s.environment,
		AppVersion:  s.appVersion,
		NowUTC:      now,
		UptimeS:     int64(now.Sub(s.startedAt).Seconds()),
		Runtime: adminRuntimeDetails{
			GoVersion:    runtime.Version(),
			NumGoroutine: runtime.NumGoroutine(),
		},
	}

	stats := s.pool.Stat()
	resp.Database.MaxConns = stats.MaxConns()
	resp.Database.TotalConns = stats.TotalConns()
	resp.Database.IdleConns = stats.IdleConns()
	resp.Database.AcquiredConns = stats.AcquiredConns()

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	pingStart := time.Now()
	err := s.pool.Ping(pingCtx)
	resp.Database.PingMS = time.Since(pingStart).Milliseconds()
	resp.Database.Reachable = err == nil
	return resp, nil
}

type adminDerivationVersionResponse struct {
	DerivationName string    `json:"derivation_name"`
	ActiveVersion  int       `json:"active_version"`
	TargetVersion  int       `json:"target_version"`
	CompiledVersion int      `json:"compiled_version"`
	Description    string    `json:"description"`
	Status         string    `json:"status"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (s *adminService) GetDerivationVersions(ctx context.Context) ([]adminDerivationVersionResponse, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			a.derivation_name,
			a.active_version,
			a.target_version,
			COALESCE(vm.max_known_version, a.target_version) AS compiled_version,
			COALESCE(vt.description, a.description) AS description,
			a.updated_at
		FROM derivation_active_versions a
		LEFT JOIN (
			SELECT projection_name, MAX(version) AS max_known_version
			FROM derivation_versions
			GROUP BY projection_name
		) vm ON vm.projection_name = a.derivation_name
		LEFT JOIN derivation_versions vt
			ON vt.projection_name = a.derivation_name
		   AND vt.version = a.target_version
		ORDER BY a.derivation_name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list derivation versions: %w", err)
	}
	defer rows.Close()

	out := make([]adminDerivationVersionResponse, 0)
	for rows.Next() {
		var row adminDerivationVersionResponse
		if err := rows.Scan(
			&row.DerivationName,
			&row.ActiveVersion,
			&row.TargetVersion,
			&row.CompiledVersion,
			&row.Description,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan derivation version row: %w", err)
		}
		row.Status = "aligned"
		if row.ActiveVersion != row.TargetVersion {
			row.Status = "rebuild_pending"
		}
		row.UpdatedAt = row.UpdatedAt.UTC()
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read derivation version rows: %w", err)
	}
	return out, nil
}

func scanAdminRebuildRun(row interface{ Scan(dest ...any) error }) (adminRebuildRunResponse, error) {
	var out adminRebuildRunResponse
	var scopeEventID *string
	var scopePubkey *string
	err := row.Scan(
		&out.ID,
		&out.DerivationName,
		&out.TargetVersion,
		&out.Scope.Type,
		&scopeEventID,
		&scopePubkey,
		&out.Scope.StartCreatedAt,
		&out.Scope.EndCreatedAt,
		&out.Status,
		&out.JobID,
		&out.Attempts,
		&out.StartedAt,
		&out.FinishedAt,
		&out.LastError,
	)
	if err != nil {
		return out, fmt.Errorf("scan rebuild run: %w", err)
	}
	if scopeEventID != nil {
		out.Scope.EventID = *scopeEventID
	}
	if scopePubkey != nil {
		out.Scope.Pubkey = *scopePubkey
	}
	if out.StartedAt != nil {
		utc := out.StartedAt.UTC()
		out.StartedAt = &utc
	}
	if out.FinishedAt != nil {
		utc := out.FinishedAt.UTC()
		out.FinishedAt = &utc
	}
	return out, nil
}

func asAdminRebuildRun(run derivation.ProjectionRebuildRun) adminRebuildRunResponse {
	return adminRebuildRunResponse{
		ID:             run.ID,
		DerivationName: run.DerivationName,
		TargetVersion:  run.TargetVersion,
		Scope:          run.Scope,
		Status:         run.Status,
		JobID:          run.JobID,
		Attempts:       run.Attempts,
		StartedAt:      run.StartedAt,
		FinishedAt:     run.FinishedAt,
		LastError:      run.LastError,
	}
}
