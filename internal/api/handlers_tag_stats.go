package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

func (h Handlers) GetEventLabels(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT labeler_pubkey, namespace, label, is_self, target_type, target_value
		FROM event_labels
		WHERE event_id = $1 OR (target_type = 'e' AND target_value = $1)
		ORDER BY is_self DESC, namespace ASC, label ASC
		LIMIT 100
	`, eventID)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	labels := make([]map[string]any, 0)
	for rows.Next() {
		var labeler, namespace, label, targetType, targetValue string
		var self bool
		if err := rows.Scan(&labeler, &namespace, &label, &self, &targetType, &targetValue); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		labels = append(labels, map[string]any{
			"labeler_pubkey": labeler,
			"namespace":      namespace,
			"label":          label,
			"is_self":        self,
			"target_type":    targetType,
			"target_value":   targetValue,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":    eventID,
		"labels":      labels,
		"consistency": "eventual",
	})
}

func (h Handlers) GetProfileExternalIdentities(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT platform, identity, proof
		FROM profile_external_identities
		WHERE pubkey = $1
		ORDER BY platform ASC
	`, pubkey)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	identities := make([]map[string]any, 0)
	for rows.Next() {
		var platform, identity, proof string
		if err := rows.Scan(&platform, &identity, &proof); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		identities = append(identities, map[string]any{
			"platform": platform,
			"identity": identity,
			"proof":    proof,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":      pubkey,
		"identities":  identities,
		"consistency": "eventual",
	})
}

func (h Handlers) GetLabelStats(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT namespace, label, count(*)::bigint
		FROM event_labels
		GROUP BY namespace, label
		ORDER BY count(*) DESC, namespace ASC, label ASC
		LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	labels := make([]map[string]any, 0)
	for rows.Next() {
		var namespace, label string
		var count int64
		if err := rows.Scan(&namespace, &label, &count); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		labels = append(labels, map[string]any{
			"namespace": namespace,
			"label":     label,
			"count":     count,
		})
	}
	var agree, disagree int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT COALESCE(sum(agree), 0), COALESCE(sum(disagree), 0)
		FROM language_label_agreement_daily
	`).Scan(&agree, &disagree)
	writeJSON(w, http.StatusOK, map[string]any{
		"labels": labels,
		"language_agreement": map[string]any{
			"agree":    agree,
			"disagree": disagree,
		},
		"consistency": "eventual",
	})
}

func (h Handlers) GetEmojiStats(w http.ResponseWriter, r *http.Request) {
	days, ok := statsWindowDays(w, r)
	if !ok {
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT shortcode, COALESCE(sum(count), 0)::bigint
		FROM custom_emoji_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - $1::int
		GROUP BY shortcode
		ORDER BY sum(count) DESC, shortcode ASC
		LIMIT 50
	`, days)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	emoji := make([]map[string]any, 0)
	for rows.Next() {
		var code string
		var count int64
		if err := rows.Scan(&code, &count); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		emoji = append(emoji, map[string]any{"shortcode": code, "count": count})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"window":      statsWindowLabel(days),
		"emoji":       emoji,
		"consistency": "eventual",
	})
}

func (h Handlers) attachLiveChatCounts(ctx context.Context, items []json.RawMessage) []json.RawMessage {
	if h.pool == nil || len(items) == 0 {
		return items
	}
	parsed := make([]map[string]any, len(items))
	addresses := make([]string, 0, len(items))
	for i, raw := range items {
		var ev map[string]any
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		parsed[i] = ev
		if address := liveActivityAddress(ev); address != "" {
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		return items
	}
	rows, err := h.pool.Query(ctx, `
		SELECT address, chat_count, unique_chatters
		FROM live_activity_stats
		WHERE address = ANY($1::text[])
	`, addresses)
	if err != nil {
		return items
	}
	defer rows.Close()
	type counts struct{ chats, chatters int64 }
	byAddress := map[string]counts{}
	for rows.Next() {
		var address string
		var row counts
		if err := rows.Scan(&address, &row.chats, &row.chatters); err != nil {
			return items
		}
		byAddress[address] = row
	}
	out := make([]json.RawMessage, len(items))
	for i, raw := range items {
		ev := parsed[i]
		if ev == nil {
			out[i] = raw
			continue
		}
		row := byAddress[liveActivityAddress(ev)]
		ev["chat_count"] = row.chats
		ev["chatters"] = row.chatters
		encoded, err := json.Marshal(ev)
		if err != nil {
			out[i] = raw
			continue
		}
		out[i] = encoded
	}
	return out
}

func liveActivityAddress(ev map[string]any) string {
	pubkey, _ := ev["pubkey"].(string)
	tags, _ := ev["tags"].([]any)
	dTag := ""
	for _, raw := range tags {
		tag, _ := raw.([]any)
		if len(tag) >= 2 {
			name, _ := tag[0].(string)
			value, _ := tag[1].(string)
			if name == "d" {
				dTag = value
				break
			}
		}
	}
	if pubkey == "" || dTag == "" {
		return ""
	}
	return "30311:" + pubkey + ":" + dTag
}

func (h Handlers) attachLongFormAttributes(ctx context.Context, items []json.RawMessage) []json.RawMessage {
	if h.pool == nil || len(items) == 0 {
		return items
	}
	ids := make([]string, 0, len(items))
	parsed := make([]map[string]any, len(items))
	for i, raw := range items {
		var ev map[string]any
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		parsed[i] = ev
		if id, _ := ev["id"].(string); id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return items
	}
	rows, err := h.pool.Query(ctx, `
		SELECT event_id, subject, published_at, reading_time_s
		FROM event_attributes
		WHERE event_id = ANY($1::text[])
	`, ids)
	if err != nil {
		return items
	}
	defer rows.Close()
	type extra struct {
		subject   string
		published *int64
		reading   int
	}
	byID := map[string]extra{}
	for rows.Next() {
		var row extra
		var id string
		if err := rows.Scan(&id, &row.subject, &row.published, &row.reading); err != nil {
			return items
		}
		byID[id] = row
	}
	if err := rows.Err(); err != nil {
		return items
	}
	out := make([]json.RawMessage, len(items))
	for i, raw := range items {
		ev := parsed[i]
		if ev == nil {
			out[i] = raw
			continue
		}
		id, _ := ev["id"].(string)
		if row, ok := byID[id]; ok {
			ev["subject"] = row.subject
			ev["reading_time_s"] = row.reading
			if row.published != nil {
				ev["published_at"] = *row.published
			}
		}
		encoded, err := json.Marshal(ev)
		if err != nil {
			out[i] = raw
			continue
		}
		out[i] = encoded
	}
	return out
}
