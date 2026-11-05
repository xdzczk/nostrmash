package api

import "net/http"

func (h Handlers) GetKindStats(w http.ResponseWriter, r *http.Request) {
	days, ok := statsWindowDays(w, r)
	if !ok {
		return
	}
	if !h.requireStatsPool(w, r) {
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT kind, COALESCE(sum(event_count), 0)::bigint, COALESCE(sum(unique_authors), 0)::bigint
		FROM kind_activity_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - $1::int
		GROUP BY kind
		ORDER BY kind ASC
	`, days)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	families := map[string]map[string]any{}
	for rows.Next() {
		var kind int
		var events, authors int64
		if err := rows.Scan(&kind, &events, &authors); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		family := kindFamily(kind)
		row, exists := families[family]
		if !exists {
			row = map[string]any{"family": family, "event_count": int64(0), "kinds": []map[string]any{}}
			families[family] = row
		}
		row["event_count"] = row["event_count"].(int64) + events
		kinds := row["kinds"].([]map[string]any)
		row["kinds"] = append(kinds, map[string]any{"kind": kind, "event_count": events, "unique_authors": authors})
	}
	out := make([]map[string]any, 0, len(families))
	for _, row := range families {
		out = append(out, row)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"window": statsWindowLabel(days), "families": out, "consistency": "eventual",
	})
}

func kindFamily(kind int) string {
	switch kind {
	case 1:
		return "notes"
	case 30023:
		return "long_form"
	case 20, 21, 22, 34235, 34236, 1063:
		return "media"
	case 4, 1059:
		return "dms"
	case 9734, 9735, 9321, 9041:
		return "zaps"
	case 9, 11, 39000, 39001, 39002, 39003:
		return "groups"
	case 1068, 1018:
		return "polls"
	case 30311, 1311:
		return "live"
	case 8, 30008, 30009:
		return "badges"
	case 30617, 1617, 1621:
		return "git"
	case 30818:
		return "wiki"
	case 30040, 30041:
		return "publications"
	case 31922, 31923, 31925:
		return "calendar"
	case 30402:
		return "classifieds"
	case 1222, 1244:
		return "voice"
	case 30166, 10166:
		return "relay_monitors"
	case 1984, 1985:
		return "moderation"
	}
	switch {
	case kind >= 9000 && kind <= 9022:
		return "moderation"
	case kind == 7000 || (kind >= 5000 && kind <= 6999):
		return "dvm"
	case kind == 0 || kind == 3 || (kind >= 10000 && kind <= 19999) ||
		kind == 30000 || kind == 30002 || kind == 30004 || kind == 30005 || kind == 30015:
		return "lists"
	default:
		return "other"
	}
}
