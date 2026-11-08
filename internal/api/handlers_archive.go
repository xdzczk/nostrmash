package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

func (h Handlers) GetDiscoveryArchive(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	day, formatOK := parseArchiveDay(r.PathValue("date"))
	if !formatOK {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "date must be YYYY-MM-DD")
		return
	}
	if day == "" {
		writeError(r.Context(), w, http.StatusNotFound, "not_found", "no archive for that day")
		return
	}
	var payload []byte
	err := h.pool.QueryRow(r.Context(), `
		SELECT payload
		FROM discovery_archive
		WHERE day = $1::date AND language = '' AND window_label = '24h'
	`, day).Scan(&payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "no archive for that day")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func (h Handlers) ListDiscoveryArchive(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 30, 366)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT to_char(day, 'YYYY-MM-DD')
		FROM discovery_archive
		WHERE language = '' AND window_label = '24h'
		ORDER BY day DESC
		LIMIT $1
	`, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	days := make([]string, 0)
	for rows.Next() {
		var day string
		if err := rows.Scan(&day); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		days = append(days, day)
	}
	writeJSON(w, http.StatusOK, map[string]any{"days": days})
}

func (h Handlers) GetRelayCoverageChanges(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 200)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT occurred_at, host, from_state, to_state, change_kind
		FROM relay_coverage_changes
		ORDER BY occurred_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		var occurred time.Time
		var host, from, to, kind string
		if err := rows.Scan(&occurred, &host, &from, &to, &kind); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		items = append(items, map[string]any{
			"occurred_at": occurred.UTC().Format(time.RFC3339),
			"host":        host,
			"from_state":  from,
			"to_state":    to,
			"change":      kind,
			"label":       relayregistry.CoverageLabel(kind, host),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

// parseArchiveDay returns the day, or an empty day when the date is in the
// future. formatOK is false when the path is not YYYY-MM-DD.
func parseArchiveDay(raw string) (day string, formatOK bool) {
	parsed, err := time.Parse("2006-01-02", raw)
	if err != nil || parsed.Format("2006-01-02") != raw {
		return "", false
	}
	today := time.Now().UTC()
	today = time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
	if parsed.After(today) {
		return "", true
	}
	return raw, true
}
