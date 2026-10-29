package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/relaynip11"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

type relayInfoProbe struct {
	LastProbeAt         *time.Time `json:"last_probe_at"`
	LastProbeStatus     string     `json:"last_probe_status"`
	ProbeFailRate       float64    `json:"probe_fail_rate"`
	AvgConnectLatencyMs *float64   `json:"avg_connect_latency_ms"`
	AvgEOSELatencyMs    *float64   `json:"avg_eose_latency_ms"`
	LastConnectOK       *bool      `json:"last_connect_ok"`
	LastSubscribeOK     *bool      `json:"last_subscribe_ok"`
	LastEOSEOK          *bool      `json:"last_eose_ok"`
}

type relayInfoActivity struct {
	EventCount24h int64 `json:"event_count_24h"`
	EventCount7d  int64 `json:"event_count_7d"`
}

type relayInfoResponse struct {
	RelayURL      string            `json:"relay_url"`
	DistinctUsers int               `json:"distinct_users"`
	Score         float64           `json:"score"`
	Probe         relayInfoProbe    `json:"probe"`
	Activity      relayInfoActivity `json:"activity"`
	NIP11         json.RawMessage   `json:"nip11"`
}

// GetRelayInfo returns one registry relay for a relay page: probe rollup,
// indexed event volume, and the stored NIP-11 document.
func (h Handlers) GetRelayInfo(w http.ResponseWriter, r *http.Request) {
	rawURL := strings.TrimSpace(r.URL.Query().Get("relay_url"))
	if rawURL == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "relay_url query param is required")
		return
	}
	normalized, err := relayurl.NormalizePublic(rawURL)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "relay_url is not a public relay URL")
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "not_available", "relay registry data is not available")
		return
	}

	ctx := r.Context()
	rec, err := relayregistry.NewStore(h.pool).GetRelay(ctx, relayurl.CanonicalKey(normalized))
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(ctx, w, http.StatusNotFound, "not_found", "relay not found")
		return
	}
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}

	activity, err := h.relayActivityCounts(ctx, rec.NormalizedURL)
	if err != nil {
		writeInternalError(ctx, w, err)
		return
	}
	status := ""
	if rec.LastProbeStatus != nil {
		status = string(*rec.LastProbeStatus)
	}
	writeJSON(w, http.StatusOK, relayInfoResponse{
		RelayURL:      rec.NormalizedURL,
		DistinctUsers: rec.DistinctUserRefCount,
		Score:         rec.Score,
		Probe: relayInfoProbe{
			LastProbeAt:         rec.LastProbeAt,
			LastProbeStatus:     status,
			ProbeFailRate:       rec.ProbeFailRate,
			AvgConnectLatencyMs: rec.AvgConnectLatency,
			AvgEOSELatencyMs:    rec.AvgEOSELatency,
			LastConnectOK:       rec.LastConnectOK,
			LastSubscribeOK:     rec.LastSubscribeOK,
			LastEOSEOK:          rec.LastEOSEOK,
		},
		Activity: activity,
		NIP11:    nip11Document(rec.CapabilitySummary),
	})
}

func (h Handlers) relayActivityCounts(ctx context.Context, relayURL string) (relayInfoActivity, error) {
	var activity relayInfoActivity
	err := h.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(event_count) FILTER (WHERE bucket_start >= date_trunc('hour', now() - interval '24 hours')), 0)::bigint,
			COALESCE(SUM(event_count) FILTER (WHERE bucket_start >= date_trunc('hour', now() - interval '7 days')), 0)::bigint
		FROM relay_activity_hourly
		WHERE relay_url = $1
	`, relayURL).Scan(&activity.EventCount24h, &activity.EventCount7d)
	return activity, err
}

func nip11ListFields(raw json.RawMessage) (name, icon string, authRequired, paymentRequired *bool) {
	summary, ok := decodeNIP11Summary(raw)
	if !ok || summary.Info == nil {
		return
	}
	name = summary.Info.Name
	icon = summary.Info.Icon
	if summary.Info.Limitation != nil {
		authRequired = summary.Info.Limitation.AuthRequired
		paymentRequired = summary.Info.Limitation.PaymentRequired
	}
	return
}

func nip11Document(raw json.RawMessage) json.RawMessage {
	if _, ok := decodeNIP11Summary(raw); !ok {
		return nil
	}
	return raw
}

func decodeNIP11Summary(raw json.RawMessage) (relaynip11.StoredSummary, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return relaynip11.StoredSummary{}, false
	}
	var summary relaynip11.StoredSummary
	if err := json.Unmarshal(trimmed, &summary); err != nil || summary.FetchedAt.IsZero() {
		return relaynip11.StoredSummary{}, false
	}
	return summary, true
}
