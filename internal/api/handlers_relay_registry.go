package api

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

type popularRelayEntry struct {
	NormalizedURL   string `json:"normalized_url"`
	DistinctUsers   int    `json:"distinct_users"`
	InRegistry      bool   `json:"in_registry"`
	AdmissionState  string `json:"admission_state,omitempty"`
	Name            string `json:"name,omitempty"`
	Icon            string `json:"icon,omitempty"`
	AuthRequired    *bool  `json:"auth_required,omitempty"`
	PaymentRequired *bool  `json:"payment_required,omitempty"`
}

type popularRelaysResponse struct {
	Relays []popularRelayEntry `json:"relays"`
	Total  int                 `json:"total"`
	Limit  int                 `json:"limit"`
}

type relayProbeHealthEntry struct {
	NormalizedURL   string     `json:"normalized_url"`
	AdmissionState  string     `json:"admission_state"`
	LastProbeAt     *time.Time `json:"last_probe_at,omitempty"`
	LastProbeStatus *string    `json:"last_probe_status,omitempty"`
	ProbeFailRate   float64    `json:"probe_fail_rate"`
	AvgConnectMs    *float64   `json:"avg_connect_latency_ms,omitempty"`
	AvgEOSEMs       *float64   `json:"avg_eose_latency_ms,omitempty"`
	LastConnectOK   *bool      `json:"last_connect_ok,omitempty"`
	LastSubscribeOK *bool      `json:"last_subscribe_ok,omitempty"`
	LastEOSEOK      *bool      `json:"last_eose_ok,omitempty"`
}

type relayProbeHealthResponse struct {
	Relays []relayProbeHealthEntry `json:"relays"`
	Total  int                     `json:"total"`
	Limit  int                     `json:"limit"`
}

func (h Handlers) GetPopularRelays(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "not_available", "relay registry data is not available")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 200)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	ctx := r.Context()
	rows, err := h.pool.Query(ctx, `
		SELECT pubkey, relays_json::text
		FROM relay_lists_latest
	`)
	if err != nil {
		writeError(ctx, w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()

	type relayAgg struct {
		url   string
		users map[string]struct{}
	}
	byKey := make(map[string]*relayAgg)

	for rows.Next() {
		var pubkey, relaysText string
		if err := rows.Scan(&pubkey, &relaysText); err != nil {
			continue
		}
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		var relays []string
		if err := json.Unmarshal([]byte(relaysText), &relays); err != nil {
			continue
		}
		for _, raw := range relays {
			// User relay lists routinely include client-local URLs. Rank only
			// publicly reachable relays, matching registry discovery.
			normalized, err := relayurl.NormalizePublic(raw)
			if err != nil {
				continue
			}
			key := relayurl.CanonicalKey(normalized)
			agg, ok := byKey[key]
			if !ok {
				agg = &relayAgg{url: normalized, users: make(map[string]struct{})}
				byKey[key] = agg
			}
			agg.users[pubkey] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		writeError(ctx, w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	type ranked struct {
		key   string
		url   string
		count int
	}
	all := make([]ranked, 0, len(byKey))
	for k, agg := range byKey {
		all = append(all, ranked{key: k, url: agg.url, count: len(agg.users)})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].count != all[j].count {
			return all[i].count > all[j].count
		}
		return all[i].url < all[j].url
	})
	total := len(all)
	if len(all) > limit {
		all = all[:limit]
	}

	registryStore := relayregistry.NewStore(h.pool)
	entries := make([]popularRelayEntry, 0, len(all))
	for _, r := range all {
		entry := popularRelayEntry{
			NormalizedURL: r.url,
			DistinctUsers: r.count,
		}
		if rec, err := registryStore.GetRelay(ctx, r.key); err == nil {
			entry.InRegistry = true
			entry.AdmissionState = string(rec.AdmissionState)
			entry.Name, entry.Icon, entry.AuthRequired, entry.PaymentRequired = nip11ListFields(rec.CapabilitySummary)
		}
		entries = append(entries, entry)
	}

	writeJSON(w, http.StatusOK, popularRelaysResponse{Relays: entries, Total: total, Limit: limit})
}

func (h Handlers) GetRelayProbeHealth(w http.ResponseWriter, r *http.Request) {
	// This endpoint intentionally remains a current-health summary; probe
	// history is deferred so the discovery metrics series stays the bounded
	// history surface for this change.
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "not_available", "relay registry data is not available")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 100, 500)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	ctx := r.Context()
	var probedTotal int
	if err := h.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM relay_registry WHERE last_probe_at IS NOT NULL
	`).Scan(&probedTotal); err != nil {
		writeError(ctx, w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	registryStore := relayregistry.NewStore(h.pool)
	records, err := registryStore.ListRelays(ctx, relayregistry.ListFilter{Limit: limit})
	if err != nil {
		writeError(ctx, w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	entries := make([]relayProbeHealthEntry, 0)
	for _, rec := range records {
		if rec.LastProbeAt == nil {
			continue
		}
		var probeStatus *string
		if rec.LastProbeStatus != nil {
			ps := string(*rec.LastProbeStatus)
			probeStatus = &ps
		}
		entries = append(entries, relayProbeHealthEntry{
			NormalizedURL:   rec.NormalizedURL,
			AdmissionState:  string(rec.AdmissionState),
			LastProbeAt:     rec.LastProbeAt,
			LastProbeStatus: probeStatus,
			ProbeFailRate:   rec.ProbeFailRate,
			AvgConnectMs:    rec.AvgConnectLatency,
			AvgEOSEMs:       rec.AvgEOSELatency,
			LastConnectOK:   rec.LastConnectOK,
			LastSubscribeOK: rec.LastSubscribeOK,
			LastEOSEOK:      rec.LastEOSEOK,
		})
	}

	writeJSON(w, http.StatusOK, relayProbeHealthResponse{Relays: entries, Total: probedTotal, Limit: limit})
}

type catalogRelayEntry struct {
	RelayURL            string     `json:"relay_url"`
	DistinctUsers       int        `json:"distinct_users"`
	Score               float64    `json:"score"`
	LastProbeAt         *time.Time `json:"last_probe_at"`
	LastProbeStatus     string     `json:"last_probe_status"`
	AvgConnectLatencyMs *float64   `json:"avg_connect_latency_ms"`
	AvgEOSELatencyMs    *float64   `json:"avg_eose_latency_ms"`
	Name                string     `json:"name,omitempty"`
	Icon                string     `json:"icon,omitempty"`
	AuthRequired        *bool      `json:"auth_required,omitempty"`
	PaymentRequired     *bool      `json:"payment_required,omitempty"`
}

type catalogRelaysResponse struct {
	GeneratedAt      time.Time           `json:"generated_at"`
	FreshnessSeconds int                 `json:"freshness_seconds"`
	Relays           []catalogRelayEntry `json:"relays"`
}

func (h Handlers) GetRelayCatalog(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "not_available", "relay registry data is not available")
		return
	}

	freshness := h.relayCatalogFreshness
	if freshness <= 0 {
		freshness = time.Hour
	}
	generatedAt := time.Now().UTC()
	ctx := r.Context()
	records, err := relayregistry.NewStore(h.pool).ListRelayCatalog(ctx, generatedAt.Add(-freshness), limit)
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}

	entries := make([]catalogRelayEntry, 0, len(records))
	for _, rec := range records {
		status := ""
		if rec.LastProbeStatus != nil {
			status = string(*rec.LastProbeStatus)
		}
		name, icon, authRequired, paymentRequired := nip11ListFields(rec.CapabilitySummary)
		entries = append(entries, catalogRelayEntry{
			RelayURL:            rec.NormalizedURL,
			DistinctUsers:       rec.DistinctUserRefCount,
			Score:               rec.Score,
			LastProbeAt:         rec.LastProbeAt,
			LastProbeStatus:     status,
			AvgConnectLatencyMs: rec.AvgConnectLatency,
			AvgEOSELatencyMs:    rec.AvgEOSELatency,
			Name:                name,
			Icon:                icon,
			AuthRequired:        authRequired,
			PaymentRequired:     paymentRequired,
		})
	}
	writeJSON(w, http.StatusOK, catalogRelaysResponse{
		GeneratedAt:      generatedAt,
		FreshnessSeconds: int(freshness.Seconds()),
		Relays:           entries,
	})
}
