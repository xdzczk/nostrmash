package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/relayadmission"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

type adminRelayRegistryEntry struct {
	URLKey               string     `json:"url_key"`
	NormalizedURL        string     `json:"normalized_url"`
	SourceSeed           bool       `json:"source_seed"`
	SourceUserList       bool       `json:"source_user_list"`
	SourceManual         bool       `json:"source_manual"`
	ManualPolicy         string     `json:"manual_policy"`
	AdmissionState       string     `json:"admission_state"`
	Score                float64    `json:"score"`
	DistinctUserRefCount int        `json:"distinct_user_ref_count"`
	WeightedUserRefScore float64    `json:"weighted_user_ref_score"`
	LastProbeAt          *time.Time `json:"last_probe_at,omitempty"`
	LastProbeStatus      *string    `json:"last_probe_status,omitempty"`
	ProbeFailRate        float64    `json:"probe_fail_rate"`
	YieldScore           float64    `json:"yield_score"`
	DuplicateRatio       float64    `json:"duplicate_ratio"`
	DiscoveredAt         time.Time  `json:"discovered_at"`
	LastSeenAt           time.Time  `json:"last_seen_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

type adminRelayRegistryResponse struct {
	Relays []adminRelayRegistryEntry `json:"relays"`
}

type adminRelayRegistryDesiredResponse struct {
	RelayURLs []string `json:"relay_urls"`
}

type adminSetPolicyRequest struct {
	RelayURL string `json:"relay_url"`
	Policy   string `json:"policy"`
}

type adminRelayDiagnosticsResponse struct {
	Relay              *adminRelayRegistryEntry     `json:"relay,omitempty"`
	RecentObservations []adminProbeObservationEntry `json:"recent_observations"`
	ScoreComponents    map[string]float64           `json:"score_components,omitempty"`
}

type adminProbeObservationEntry struct {
	ProbedAt         time.Time `json:"probed_at"`
	ConnectOK        bool      `json:"connect_ok"`
	SubscribeOK      bool      `json:"subscribe_ok"`
	EOSEOK           bool      `json:"eose_ok"`
	ConnectLatencyMs *float64  `json:"connect_latency_ms,omitempty"`
	EOSELatencyMs    *float64  `json:"eose_latency_ms,omitempty"`
	ErrorCode        *string   `json:"error_code,omitempty"`
	ErrorTextShort   *string   `json:"error_text_short,omitempty"`
}

func (h AdminHandlers) GetRelayRegistry(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 100, 500)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	resp, err := h.service.GetRelayRegistry(r.Context(), limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h AdminHandlers) GetRelayRegistryDesired(w http.ResponseWriter, r *http.Request) {
	resp, err := h.service.GetRelayRegistryDesired(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h AdminHandlers) SetRelayRegistryPolicy(w http.ResponseWriter, r *http.Request) {
	var req adminSetPolicyRequest
	if !decodeJSONBodyLimited(w, r, adminBodyLimitBytes, &req, true) {
		return
	}
	req.RelayURL = strings.TrimSpace(req.RelayURL)
	req.Policy = strings.TrimSpace(strings.ToLower(req.Policy))
	if req.RelayURL == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "relay_url is required")
		return
	}
	if !relayregistry.ManualPolicy(req.Policy).Valid() {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "policy must be one of: none, pinned, blocked, drained")
		return
	}
	if err := h.service.SetRelayRegistryPolicy(r.Context(), req); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *adminService) GetRelayRegistry(ctx context.Context, limit int) (adminRelayRegistryResponse, error) {
	registryStore := relayregistry.NewStore(s.pool)
	records, err := registryStore.ListRelays(ctx, relayregistry.ListFilter{Limit: limit})
	if err != nil {
		return adminRelayRegistryResponse{}, fmt.Errorf("list relay registry: %w", err)
	}
	entries := make([]adminRelayRegistryEntry, 0, len(records))
	for _, rec := range records {
		var probeStatus *string
		if rec.LastProbeStatus != nil {
			s := string(*rec.LastProbeStatus)
			probeStatus = &s
		}
		entries = append(entries, adminRelayRegistryEntry{
			URLKey:               rec.URLKey,
			NormalizedURL:        rec.NormalizedURL,
			SourceSeed:           rec.SourceSeed,
			SourceUserList:       rec.SourceUserList,
			SourceManual:         rec.SourceManual,
			ManualPolicy:         string(rec.ManualPolicy),
			AdmissionState:       string(rec.AdmissionState),
			Score:                rec.Score,
			DistinctUserRefCount: rec.DistinctUserRefCount,
			WeightedUserRefScore: rec.WeightedUserRefScore,
			LastProbeAt:          rec.LastProbeAt,
			LastProbeStatus:      probeStatus,
			ProbeFailRate:        rec.ProbeFailRate,
			YieldScore:           rec.YieldScore,
			DuplicateRatio:       rec.DuplicateRatio,
			DiscoveredAt:         rec.DiscoveredAt,
			LastSeenAt:           rec.LastSeenAt,
			UpdatedAt:            rec.UpdatedAt,
		})
	}
	return adminRelayRegistryResponse{Relays: entries}, nil
}

func (s *adminService) GetRelayRegistryDesired(ctx context.Context) (adminRelayRegistryDesiredResponse, error) {
	registryStore := relayregistry.NewStore(s.pool)
	urls, err := registryStore.GetDesiredActiveRelays(ctx)
	if err != nil {
		return adminRelayRegistryDesiredResponse{}, fmt.Errorf("get desired relays: %w", err)
	}
	if urls == nil {
		urls = []string{}
	}
	return adminRelayRegistryDesiredResponse{RelayURLs: urls}, nil
}

func (s *adminService) SetRelayRegistryPolicy(ctx context.Context, req adminSetPolicyRequest) error {
	normalized, err := relayurl.Normalize(req.RelayURL, relayurl.NormalizeOptions{})
	if err != nil {
		return fmt.Errorf("invalid relay URL: %w", err)
	}
	urlKey := relayurl.CanonicalKey(normalized)
	registryStore := relayregistry.NewStore(s.pool)

	_, getErr := registryStore.GetRelay(ctx, urlKey)
	if getErr != nil {
		if err := registryStore.UpsertSeedRelay(ctx, urlKey, normalized); err != nil {
			return err
		}
	}

	return registryStore.SetManualPolicy(ctx, urlKey, relayregistry.ManualPolicy(req.Policy))
}

func (h AdminHandlers) GetRelayDiagnostics(w http.ResponseWriter, r *http.Request) {
	relayURL := strings.TrimSpace(r.URL.Query().Get("relay_url"))
	if relayURL == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "relay_url query param is required")
		return
	}
	resp, err := h.service.GetRelayDiagnostics(r.Context(), relayURL)
	if err != nil {
		writeError(r.Context(), w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *adminService) GetRelayDiagnostics(ctx context.Context, rawURL string) (adminRelayDiagnosticsResponse, error) {
	normalized, err := relayurl.Normalize(rawURL, relayurl.NormalizeOptions{})
	if err != nil {
		return adminRelayDiagnosticsResponse{}, fmt.Errorf("invalid relay URL: %w", err)
	}
	urlKey := relayurl.CanonicalKey(normalized)
	registryStore := relayregistry.NewStore(s.pool)

	rec, err := registryStore.GetRelay(ctx, urlKey)
	if err != nil {
		return adminRelayDiagnosticsResponse{}, fmt.Errorf("relay not found: %w", err)
	}

	var probeStatus *string
	if rec.LastProbeStatus != nil {
		ps := string(*rec.LastProbeStatus)
		probeStatus = &ps
	}
	entry := adminRelayRegistryEntry{
		URLKey:               rec.URLKey,
		NormalizedURL:        rec.NormalizedURL,
		SourceSeed:           rec.SourceSeed,
		SourceUserList:       rec.SourceUserList,
		SourceManual:         rec.SourceManual,
		ManualPolicy:         string(rec.ManualPolicy),
		AdmissionState:       string(rec.AdmissionState),
		Score:                rec.Score,
		DistinctUserRefCount: rec.DistinctUserRefCount,
		WeightedUserRefScore: rec.WeightedUserRefScore,
		LastProbeAt:          rec.LastProbeAt,
		LastProbeStatus:      probeStatus,
		ProbeFailRate:        rec.ProbeFailRate,
		YieldScore:           rec.YieldScore,
		DuplicateRatio:       rec.DuplicateRatio,
		DiscoveredAt:         rec.DiscoveredAt,
		LastSeenAt:           rec.LastSeenAt,
		UpdatedAt:            rec.UpdatedAt,
	}

	observations, err := registryStore.ListRecentObservations(ctx, urlKey, 20)
	if err != nil {
		return adminRelayDiagnosticsResponse{}, fmt.Errorf("list observations: %w", err)
	}
	obsEntries := make([]adminProbeObservationEntry, 0, len(observations))
	for _, o := range observations {
		obsEntries = append(obsEntries, adminProbeObservationEntry{
			ProbedAt:         o.ProbedAt,
			ConnectOK:        o.ConnectOK,
			SubscribeOK:      o.SubscribeOK,
			EOSEOK:           o.EOSEOK,
			ConnectLatencyMs: o.ConnectLatencyMs,
			EOSELatencyMs:    o.EOSELatencyMs,
			ErrorCode:        o.ErrorCode,
			ErrorTextShort:   o.ErrorTextShort,
		})
	}

	var scoreComponents map[string]float64
	if rec.ScoreComponents != nil {
		_ = json.Unmarshal(rec.ScoreComponents, &scoreComponents)
	}

	return adminRelayDiagnosticsResponse{
		Relay:              &entry,
		RecentObservations: obsEntries,
		ScoreComponents:    scoreComponents,
	}, nil
}

type adminAdmissionDryRunResponse struct {
	Proposals []adminAdmissionProposal `json:"proposals"`
}

type adminAdmissionProposal struct {
	URLKey        string                         `json:"url_key"`
	NormalizedURL string                         `json:"normalized_url"`
	CurrentState  string                         `json:"current_state"`
	ProposedState string                         `json:"proposed_state"`
	Score         relayadmission.ScoreComponents `json:"score"`
	Changed       bool                           `json:"changed"`
}

func (h AdminHandlers) GetRelayAdmissionDryRun(w http.ResponseWriter, r *http.Request) {
	resp, err := h.service.GetRelayAdmissionDryRun(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *adminService) GetRelayAdmissionDryRun(ctx context.Context) (adminAdmissionDryRunResponse, error) {
	registryStore := relayregistry.NewStore(s.pool)
	admCtrl := relayadmission.NewController(nil, registryStore, s.admissionCfg)
	proposals, err := admCtrl.RunDryRun(ctx)
	if err != nil {
		return adminAdmissionDryRunResponse{}, fmt.Errorf("dry run: %w", err)
	}
	out := make([]adminAdmissionProposal, 0, len(proposals))
	for _, p := range proposals {
		out = append(out, adminAdmissionProposal{
			URLKey:        p.URLKey,
			NormalizedURL: p.NormalizedURL,
			CurrentState:  string(p.CurrentState),
			ProposedState: string(p.ProposedState),
			Score:         p.Score,
			Changed:       p.Changed,
		})
	}
	return adminAdmissionDryRunResponse{Proposals: out}, nil
}
