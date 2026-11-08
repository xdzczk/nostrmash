package api

import (
	"context"
	"net/http"
	"strings"
	"time"
)

func (h Handlers) GetConversationStats(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT day, threads, p50_first_reply_s, avg_depth_x100, avg_branching_x100
		FROM thread_shape_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - 30
		ORDER BY day ASC
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	series := make([]map[string]any, 0)
	for rows.Next() {
		var day time.Time
		var threads, p50, depth, branching int64
		if err := rows.Scan(&day, &threads, &p50, &depth, &branching); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		series = append(series, map[string]any{
			"day": day.Format("2006-01-02"), "threads": threads,
			"p50_first_reply_s": p50, "avg_depth_x100": depth, "avg_branching_x100": branching,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"series": series})
}

func (h Handlers) GetZapLeaderboard(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	role := strings.TrimSpace(r.URL.Query().Get("role"))
	if role == "" {
		role = "receiver"
	}
	if role != "sender" && role != "receiver" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "role must be sender or receiver")
		return
	}
	days, ok := statsWindowDays(w, r)
	if !ok {
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT pubkey, sum(sats)::bigint, sum(zap_count)::bigint
		FROM zap_leaderboard_daily
		WHERE role = $1 AND day >= (now() AT TIME ZONE 'utc')::date - $2::int
		GROUP BY pubkey
		ORDER BY sum(sats) DESC
		LIMIT 100
	`, role, days)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		var pubkey string
		var sats, count int64
		if err := rows.Scan(&pubkey, &sats, &count); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		items = append(items, map[string]any{"pubkey": pubkey, "sats": sats, "zap_count": count})
	}
	writeJSON(w, http.StatusOK, map[string]any{"role": role, "window": statsWindowLabel(days), "items": items})
}

func (h Handlers) GetZapHeatmap(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT day_of_week, hour, zap_count, sats FROM zap_hourly ORDER BY day_of_week, hour
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	cells := make([]map[string]any, 0)
	for rows.Next() {
		var dow, hour int
		var count, sats int64
		if err := rows.Scan(&dow, &hour, &count, &sats); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		cells = append(cells, map[string]any{"day_of_week": dow, "hour": hour, "zap_count": count, "sats": sats})
	}
	writeJSON(w, http.StatusOK, map[string]any{"cells": cells})
}

func (h Handlers) GetEmergingHashtags(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT hashtag, z_score_7d, status, half_life_days
		FROM hashtag_momentum
		WHERE status = 'emerging'
		ORDER BY z_score_7d DESC
		LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		var tag, status string
		var score, half float64
		if err := rows.Scan(&tag, &score, &status, &half); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		items = append(items, map[string]any{"hashtag": tag, "z_score_7d": score, "status": status, "half_life_days": half})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handlers) GetEmergingDomains(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT domain, z_score_7d, status, half_life_days
		FROM domain_momentum
		WHERE status = 'emerging'
		ORDER BY z_score_7d DESC
		LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		var domain, status string
		var score, half float64
		if err := rows.Scan(&domain, &score, &status, &half); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		items = append(items, map[string]any{"domain": domain, "z_score_7d": score, "status": status, "half_life_days": half})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handlers) GetHashtagHistory(w http.ResponseWriter, r *http.Request) {
	tag := strings.ToLower(strings.TrimSpace(r.PathValue("tag")))
	if tag == "" || h.pool == nil {
		if tag == "" {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "hashtag is required")
			return
		}
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT day, events, authors FROM hashtag_daily
		WHERE hashtag = $1 AND day >= (now() AT TIME ZONE 'utc')::date - 90
		ORDER BY day ASC
	`, tag)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	series := make([]map[string]any, 0)
	for rows.Next() {
		var day time.Time
		var events, authors int64
		if err := rows.Scan(&day, &events, &authors); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		series = append(series, map[string]any{"day": day.Format("2006-01-02"), "events": events, "authors": authors})
	}
	writeJSON(w, http.StatusOK, map[string]any{"hashtag": tag, "series": series})
}

func (h Handlers) GetDeletionStats(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT day, kind, count,
		       CASE WHEN count = 0 THEN 0 ELSE latency_total_s / count END
		FROM deletion_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - 30
		ORDER BY day DESC, count DESC
		LIMIT 200
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		var day time.Time
		var kind int
		var count, p50 int64
		if err := rows.Scan(&day, &kind, &count, &p50); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		items = append(items, map[string]any{
			"day": day.Format("2006-01-02"), "kind": kind, "count": count, "p50_time_to_delete_s": p50,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handlers) GetModerationStats(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	volume, err := h.dailyTypedCounts(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT target_type, target_value, report_count, reporter_count
		FROM report_target_counts
		ORDER BY report_count DESC
		LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	reported := make([]map[string]any, 0)
	for rows.Next() {
		var kind, value string
		var reports, reporters int64
		if err := rows.Scan(&kind, &value, &reports, &reporters); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		reported = append(reported, map[string]any{
			"target_type": kind, "target_value": value, "report_count": reports, "reporter_count": reporters,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"volume": volume, "most_reported": reported})
}

func (h Handlers) dailyTypedCounts(ctx context.Context) ([]map[string]any, error) {
	rows, err := h.pool.Query(ctx, `
		SELECT day, report_type, count FROM report_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - 30
		ORDER BY day ASC, count DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var day time.Time
		var reportType string
		var count int64
		if err := rows.Scan(&day, &reportType, &count); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"day": day.Format("2006-01-02"), "report_type": reportType, "count": count})
	}
	return out, rows.Err()
}

func (h Handlers) GetProfileModeration(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if len(pubkey) != 64 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	var muted, reports, rate int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT muted_by_count, reports_received, mute_rate_bps
		FROM profile_moderation_stats WHERE pubkey = $1
	`, pubkey).Scan(&muted, &reports, &rate)
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey": pubkey, "muted_by_count": muted, "reports_received": reports, "mute_rate_bps": rate,
	})
}

func (h Handlers) GetSpamStats(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT fingerprint, first_event_id, first_pubkey, event_count, pubkey_count
		FROM content_fingerprints
		WHERE event_count > 1 AND pubkey_count > 1
		ORDER BY event_count DESC
		LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	clusters := make([]map[string]any, 0)
	for rows.Next() {
		var fingerprint, eventID, pubkey string
		var events, authors int64
		if err := rows.Scan(&fingerprint, &eventID, &pubkey, &events, &authors); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		clusters = append(clusters, map[string]any{
			"fingerprint": fingerprint, "first_event_id": eventID, "first_pubkey": pubkey,
			"event_count": events, "pubkey_count": authors,
		})
	}
	bursts, err := h.pool.Query(r.Context(), `
		SELECT pubkey, activity_date, hour_of_day, post_count
		FROM author_hourly_activity
		WHERE activity_date >= (now() AT TIME ZONE 'utc')::date - 1
		  AND post_count >= 30
		ORDER BY post_count DESC
		LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer bursts.Close()
	authors := make([]map[string]any, 0)
	for bursts.Next() {
		var pubkey string
		var day time.Time
		var hour int
		var posts int64
		if err := bursts.Scan(&pubkey, &day, &hour, &posts); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		authors = append(authors, map[string]any{
			"pubkey": pubkey, "day": day.Format("2006-01-02"), "hour": hour, "post_count": posts,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"duplicate_clusters": clusters, "burst_authors": authors})
}

func (h Handlers) GetRelayListStats(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT day, lists, avg_relays_x100, p50_relays, read_only_share_bps, write_only_share_bps
		FROM relay_list_stats_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - 30
		ORDER BY day ASC
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	series := make([]map[string]any, 0)
	for rows.Next() {
		var day time.Time
		var lists, avg, p50, readOnly, writeOnly int64
		if err := rows.Scan(&day, &lists, &avg, &p50, &readOnly, &writeOnly); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		series = append(series, map[string]any{
			"day": day.Format("2006-01-02"), "lists": lists, "avg_relays_x100": avg,
			"p50_relays": p50, "read_only_share_bps": readOnly, "write_only_share_bps": writeOnly,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"series": series})
}

func (h Handlers) GetRelayHealthHistory(w http.ResponseWriter, r *http.Request) {
	host := strings.TrimSpace(r.PathValue("relayHost"))
	if host == "" || h.pool == nil {
		if host == "" {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "relay host is required")
			return
		}
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT h.day, h.probes, h.connect_ok_bps, h.connect_p50_ms, h.connect_p90_ms, h.eose_p50_ms
		FROM relay_health_daily h
		JOIN relay_registry r ON r.url_key = h.url_key
		WHERE split_part(r.normalized_url, '/', 3) = $1
		  AND h.day >= (now() AT TIME ZONE 'utc')::date - 30
		ORDER BY h.day ASC
	`, host)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	series := make([]map[string]any, 0)
	for rows.Next() {
		var day time.Time
		var probes, okBps, p50, p90, eose int64
		if err := rows.Scan(&day, &probes, &okBps, &p50, &p90, &eose); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		series = append(series, map[string]any{
			"day": day.Format("2006-01-02"), "probes": probes, "connect_ok_bps": okBps,
			"connect_p50_ms": p50, "connect_p90_ms": p90, "eose_p50_ms": eose,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"relay_host": host, "series": series})
}

func (h Handlers) GetRelaySoftwareStats(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT COALESCE(NULLIF(capability_summary_json->'info'->>'software', ''), 'unknown'), count(*)::bigint
		FROM relay_registry
		GROUP BY 1
		ORDER BY count(*) DESC
		LIMIT 30
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	software := make([]map[string]any, 0)
	for rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		software = append(software, map[string]any{"software": name, "relays": count})
	}
	var authShare, paymentShare int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT
			CASE WHEN count(*) = 0 THEN 0
			     ELSE (count(*) FILTER (WHERE COALESCE(capability_summary_json->'info'->'limitation'->>'auth_required', '') IN ('true', 'yes')) * 10000) / count(*)
			END,
			CASE WHEN count(*) = 0 THEN 0
			     ELSE (count(*) FILTER (WHERE COALESCE(capability_summary_json->'info'->'limitation'->>'payment_required', '') IN ('true', 'yes')) * 10000) / count(*)
			END
		FROM relay_registry
	`).Scan(&authShare, &paymentShare)
	writeJSON(w, http.StatusOK, map[string]any{
		"software": software, "auth_required_share_bps": authShare, "payment_required_share_bps": paymentShare,
	})
}
