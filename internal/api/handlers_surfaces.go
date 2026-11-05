package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (h Handlers) requireStatsPool(w http.ResponseWriter, r *http.Request) bool {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return false
	}
	return true
}

func (h Handlers) GetGroups(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	sort := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort")))
	order := "g.updated_at DESC"
	switch sort {
	case "", "recent":
	case "members":
		order = "g.member_count DESC, g.updated_at DESC"
	case "active":
		order = "messages_7d DESC, g.updated_at DESC"
	default:
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "sort must be active, members, or recent")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT g.relay_url, g.group_id, g.name, g.about, g.is_public, g.is_open,
		       g.admin_count, g.member_count,
		       COALESCE((
		           SELECT sum(message_count) FROM group_activity_daily a
		           WHERE a.relay_url = g.relay_url AND a.group_id = g.group_id
		             AND a.day >= (now() AT TIME ZONE 'utc')::date - 7
		       ), 0)::bigint AS messages_7d
		FROM groups_latest g
		ORDER BY `+order+`
		LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	writeJSON(w, http.StatusOK, map[string]any{"groups": scanGroups(rows), "consistency": "eventual"})
}

func (h Handlers) GetGroup(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	host, groupID := r.PathValue("relayHost"), r.PathValue("groupId")
	var name, about, relayURL string
	var public, open bool
	var admins, members int
	err := h.pool.QueryRow(r.Context(), `
		SELECT relay_url, name, about, is_public, is_open, admin_count, member_count
		FROM groups_latest
		WHERE group_id = $2 AND split_part(relay_url, '/', 3) = $1
	`, host, groupID).Scan(&relayURL, &name, &about, &public, &open, &admins, &members)
	if err != nil {
		writeError(r.Context(), w, http.StatusNotFound, "not_found", "group not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"relay_url": relayURL, "group_id": groupID, "name": name, "about": about,
		"is_public": public, "is_open": open, "admin_count": admins, "member_count": members,
		"consistency": "eventual",
	})
}

func (h Handlers) GetGroupMessages(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT event_id, author, created_at, kind
		FROM group_messages
		WHERE group_id = $2 AND split_part(relay_url, '/', 3) = $1
		ORDER BY created_at DESC
		LIMIT 50
	`, r.PathValue("relayHost"), r.PathValue("groupId"))
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	messages := make([]map[string]any, 0)
	for rows.Next() {
		var id, author string
		var created int64
		var kind int
		if err := rows.Scan(&id, &author, &created, &kind); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		messages = append(messages, map[string]any{"event_id": id, "author": author, "created_at": created, "kind": kind})
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": messages, "consistency": "eventual"})
}

func (h Handlers) GetRelayGroups(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT relay_url, group_id, name, about, is_public, is_open, admin_count, member_count, 0::bigint
		FROM groups_latest
		WHERE split_part(relay_url, '/', 3) = $1
		ORDER BY member_count DESC, group_id ASC
		LIMIT 100
	`, r.PathValue("relayHost"))
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	writeJSON(w, http.StatusOK, map[string]any{"groups": scanGroups(rows), "consistency": "eventual"})
}

func (h Handlers) GetGroupStats(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	var groups, active1, active7, messages int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT
		    (SELECT count(*) FROM groups_latest),
		    (SELECT count(DISTINCT (relay_url, group_id)) FROM group_activity_daily WHERE day >= (now() AT TIME ZONE 'utc')::date - 1 AND message_count > 0),
		    (SELECT count(DISTINCT (relay_url, group_id)) FROM group_activity_daily WHERE day >= (now() AT TIME ZONE 'utc')::date - 7 AND message_count > 0),
		    (SELECT COALESCE(sum(message_count), 0) FROM group_activity_daily WHERE day >= (now() AT TIME ZONE 'utc')::date - 1)
	`).Scan(&groups, &active1, &active7, &messages)
	series := make([]map[string]any, 0)
	if rows, err := h.pool.Query(r.Context(), `
		SELECT day, sum(message_count)::bigint, count(DISTINCT (relay_url, group_id))::bigint
		FROM group_activity_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - 30
		GROUP BY day ORDER BY day ASC
	`); err == nil {
		defer rows.Close()
		for rows.Next() {
			var day time.Time
			var count, active int64
			if err := rows.Scan(&day, &count, &active); err == nil {
				series = append(series, map[string]any{"day": day.Format("2006-01-02"), "messages": count, "active_groups": active})
			}
		}
	}
	histogram := make([]map[string]any, 0)
	if rows, err := h.pool.Query(r.Context(), `
		SELECT width_bucket(member_count, ARRAY[1, 10, 50, 200, 1000]), count(*)::bigint
		FROM groups_latest
		GROUP BY 1 ORDER BY 1
	`); err == nil {
		defer rows.Close()
		labels := []string{"0", "1_9", "10_49", "50_199", "200_999", "1000_plus"}
		for rows.Next() {
			var bucket int
			var count int64
			if err := rows.Scan(&bucket, &count); err == nil && bucket >= 0 && bucket < len(labels) {
				histogram = append(histogram, map[string]any{"members": labels[bucket], "groups": count})
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"groups": groups, "active_groups_1d": active1, "active_groups_7d": active7,
		"messages_1d": messages, "series": series, "size_histogram": histogram,
		"consistency": "eventual",
	})
}

func (h Handlers) GetLiveChat(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	address := r.PathValue("address")
	var chats, chatters, last int64
	if err := h.pool.QueryRow(r.Context(), `
		SELECT chat_count, unique_chatters, last_chat_at FROM live_activity_stats WHERE address = $1
	`, address).Scan(&chats, &chatters, &last); err != nil {
		chats, chatters, last = 0, 0, 0
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"address": address, "chat_count": chats, "unique_chatters": chatters, "last_chat_at": last,
		"consistency": "eventual",
	})
}

func (h Handlers) GetEventPoll(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	payload, ok := h.loadPoll(r, r.PathValue("id"))
	if !ok {
		writeError(r.Context(), w, http.StatusNotFound, "not_found", "poll not found")
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetPolls(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	order := "p.event_id DESC"
	if r.URL.Query().Get("sort") == "votes" {
		order = "votes DESC, p.event_id DESC"
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT p.event_id, p.author, p.question, p.poll_type, COALESCE(sum(c.votes), 0)::bigint AS votes
		FROM polls p
		LEFT JOIN poll_option_counts c ON c.poll_event_id = p.event_id
		GROUP BY p.event_id, p.author, p.question, p.poll_type
		ORDER BY `+order+`
		LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	polls := make([]map[string]any, 0)
	for rows.Next() {
		var id, author, question, pollType string
		var votes int64
		if err := rows.Scan(&id, &author, &question, &pollType, &votes); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		polls = append(polls, map[string]any{
			"event_id": id, "author": author, "question": question, "poll_type": pollType, "votes": votes,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"polls": polls, "consistency": "eventual"})
}

func (h Handlers) GetPollStats(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	var polls, votes int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT (SELECT count(*) FROM polls), (SELECT COALESCE(sum(votes), 0) FROM poll_option_counts)
	`).Scan(&polls, &votes)
	writeJSON(w, http.StatusOK, map[string]any{"polls": polls, "votes": votes, "consistency": "eventual"})
}

func (h Handlers) GetProfileBadges(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	rows, err := h.pool.Query(r.Context(), `
		SELECT definition_address, awarded_at FROM badge_awards
		WHERE recipient = $1 ORDER BY awarded_at DESC LIMIT 100
	`, pubkey)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	awards := make([]map[string]any, 0)
	for rows.Next() {
		var address string
		var at int64
		if err := rows.Scan(&address, &at); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		awards = append(awards, map[string]any{"address": address, "awarded_at": at})
	}
	var accepted []byte
	_ = h.pool.QueryRow(r.Context(), `SELECT accepted FROM profile_badges_latest WHERE pubkey = $1`, pubkey).Scan(&accepted)
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey": pubkey, "awards": awards, "accepted": jsonRawOrEmpty(accepted), "consistency": "eventual",
	})
}

func (h Handlers) GetBadge(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	address := r.PathValue("address")
	var issuer, name, description, image string
	err := h.pool.QueryRow(r.Context(), `
		SELECT issuer, name, description, image FROM badge_definitions WHERE address = $1
	`, address).Scan(&issuer, &name, &description, &image)
	if err != nil {
		writeError(r.Context(), w, http.StatusNotFound, "not_found", "badge not found")
		return
	}
	var awards int64
	_ = h.pool.QueryRow(r.Context(), `SELECT count(*) FROM badge_awards WHERE definition_address = $1`, address).Scan(&awards)
	writeJSON(w, http.StatusOK, map[string]any{
		"address": address, "issuer": issuer, "name": name, "description": description,
		"image": image, "awards": awards, "consistency": "eventual",
	})
}

func (h Handlers) GetBadgeStats(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	var definitions, awards int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT (SELECT count(*) FROM badge_definitions), (SELECT count(*) FROM badge_awards)
	`).Scan(&definitions, &awards)
	writeJSON(w, http.StatusOK, map[string]any{"definitions": definitions, "awards": awards, "consistency": "eventual"})
}

func (h Handlers) GetEventGoal(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	var author, summary string
	var amount, raised int64
	err := h.pool.QueryRow(r.Context(), `
		SELECT author, amount_msats, raised_sats, summary FROM zap_goals WHERE event_id = $1
	`, r.PathValue("id")).Scan(&author, &amount, &raised, &summary)
	if err != nil {
		writeError(r.Context(), w, http.StatusNotFound, "not_found", "goal not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id": r.PathValue("id"), "author": author, "amount_msats": amount,
		"raised_sats": raised, "summary": summary, "consistency": "eventual",
	})
}

func (h Handlers) GetGoals(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT event_id, author, amount_msats, raised_sats, summary
		FROM zap_goals ORDER BY raised_sats DESC LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	goals := make([]map[string]any, 0)
	for rows.Next() {
		var id, author, summary string
		var amount, raised int64
		if err := rows.Scan(&id, &author, &amount, &raised, &summary); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		goals = append(goals, map[string]any{
			"event_id": id, "author": author, "amount_msats": amount, "raised_sats": raised, "summary": summary,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"goals": goals, "consistency": "eventual"})
}

func (h Handlers) GetProfileStatus(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	rows, err := h.pool.Query(r.Context(), `
		SELECT d_tag, content, link, expires_at FROM user_status_latest WHERE pubkey = $1
	`, pubkey)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	statuses := make([]map[string]any, 0)
	for rows.Next() {
		var dTag, content, link string
		var expires *int64
		if err := rows.Scan(&dTag, &content, &link, &expires); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		statuses = append(statuses, map[string]any{"d_tag": dTag, "content": content, "link": link, "expires_at": expires})
	}
	writeJSON(w, http.StatusOK, map[string]any{"pubkey": pubkey, "statuses": statuses, "consistency": "eventual"})
}

func (h Handlers) GetCurationSets(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT address, author, kind, title, item_count FROM curation_sets
		ORDER BY updated_at DESC LIMIT 50
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	sets := make([]map[string]any, 0)
	for rows.Next() {
		var address, author, title string
		var kind, items int
		if err := rows.Scan(&address, &author, &kind, &title, &items); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		sets = append(sets, map[string]any{
			"address": address, "author": author, "kind": kind, "title": title, "item_count": items,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"curation_sets": sets, "consistency": "eventual"})
}

func (h Handlers) GetCurationSet(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	address := r.PathValue("address")
	var author, title, description string
	var kind, items int
	err := h.pool.QueryRow(r.Context(), `
		SELECT author, kind, title, description, item_count FROM curation_sets WHERE address = $1
	`, address).Scan(&author, &kind, &title, &description, &items)
	if err != nil {
		writeError(r.Context(), w, http.StatusNotFound, "not_found", "curation set not found")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT item_type, item_value FROM curation_set_items WHERE address = $1 ORDER BY item_type, item_value
	`, address)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	setItems := make([]map[string]any, 0)
	for rows.Next() {
		var itemType, value string
		if err := rows.Scan(&itemType, &value); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		setItems = append(setItems, map[string]any{"type": itemType, "value": value})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"address": address, "author": author, "kind": kind, "title": title,
		"description": description, "item_count": items, "items": setItems, "consistency": "eventual",
	})
}

func (h Handlers) GetProfileInterests(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	rows, err := h.pool.Query(r.Context(), `
		SELECT kind, hashtags FROM interest_lists_latest WHERE pubkey = $1 ORDER BY kind
	`, pubkey)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	lists := make([]map[string]any, 0)
	for rows.Next() {
		var kind int
		var raw []byte
		if err := rows.Scan(&kind, &raw); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		lists = append(lists, map[string]any{"kind": kind, "hashtags": jsonRawOrEmpty(raw)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"pubkey": pubkey, "interests": lists, "consistency": "eventual"})
}

func (h Handlers) GetProfileLists(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	lists := make([]map[string]any, 0)
	rows, err := h.pool.Query(r.Context(), `
		SELECT kind, relays FROM relay_preference_lists_latest WHERE pubkey = $1 ORDER BY kind
	`, pubkey)
	if err == nil {
		for rows.Next() {
			var kind int
			var raw []byte
			if err := rows.Scan(&kind, &raw); err == nil {
				lists = append(lists, map[string]any{"kind": kind, "relays": jsonRawOrEmpty(raw)})
			}
		}
		rows.Close()
	}
	writeJSON(w, http.StatusOK, map[string]any{"pubkey": pubkey, "lists": lists, "consistency": "eventual"})
}

func (h Handlers) GetMediaHostStats(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT canonical_domain, COALESCE(sum(media_count), 0)::bigint, COALESCE(sum(authors), 0)::bigint
		FROM media_host_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - 7
		GROUP BY canonical_domain
		ORDER BY sum(media_count) DESC
		LIMIT 30
	`)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	hosts := make([]map[string]any, 0)
	for rows.Next() {
		var domain string
		var media, authors int64
		if err := rows.Scan(&domain, &media, &authors); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		hosts = append(hosts, map[string]any{"canonical_domain": domain, "media_count": media, "authors": authors})
	}
	declared := make([]map[string]any, 0)
	serverRows, err := h.pool.Query(r.Context(), `
		SELECT server, count(*)::bigint
		FROM blossom_servers_latest, jsonb_array_elements_text(servers) AS server
		GROUP BY server
		ORDER BY count(*) DESC
		LIMIT 30
	`)
	if err == nil {
		for serverRows.Next() {
			var server string
			var count int64
			if err := serverRows.Scan(&server, &count); err == nil {
				declared = append(declared, map[string]any{"server": server, "pubkeys": count})
			}
		}
		serverRows.Close()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"observed": hosts, "blossom_servers": declared, "consistency": "eventual",
	})
}

func (h Handlers) GetProfileLabels(w http.ResponseWriter, r *http.Request) {
	if !h.requireStatsPool(w, r) {
		return
	}
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	rows, err := h.pool.Query(r.Context(), `
		SELECT event_id, labeler_pubkey, namespace, label
		FROM event_labels
		WHERE (target_type = 'p' AND target_value = $1) OR labeler_pubkey = $1
		ORDER BY namespace, label
		LIMIT 100
	`, pubkey)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	labels := make([]map[string]any, 0)
	for rows.Next() {
		var eventID, labeler, namespace, label string
		if err := rows.Scan(&eventID, &labeler, &namespace, &label); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		labels = append(labels, map[string]any{
			"event_id": eventID, "labeler_pubkey": labeler, "namespace": namespace, "label": label,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"pubkey": pubkey, "labels": labels, "consistency": "eventual"})
}

func scanGroups(rows pgx.Rows) []map[string]any {
	groups := make([]map[string]any, 0)
	for rows.Next() {
		var relayURL, groupID, name, about string
		var public, open bool
		var admins, members int
		var messages int64
		if err := rows.Scan(&relayURL, &groupID, &name, &about, &public, &open, &admins, &members, &messages); err != nil {
			return groups
		}
		groups = append(groups, map[string]any{
			"relay_url": relayURL, "group_id": groupID, "name": name, "about": about,
			"is_public": public, "is_open": open, "admin_count": admins, "member_count": members,
			"messages_7d": messages,
		})
	}
	return groups
}

func (h Handlers) loadPoll(r *http.Request, eventID string) (map[string]any, bool) {
	var author, question, pollType string
	var options []byte
	err := h.pool.QueryRow(r.Context(), `
		SELECT author, question, options, poll_type FROM polls WHERE event_id = $1
	`, eventID).Scan(&author, &question, &options, &pollType)
	if err != nil {
		return nil, false
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT option_id, votes FROM poll_option_counts WHERE poll_event_id = $1 ORDER BY option_id
	`, eventID)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	counts := make([]map[string]any, 0)
	for rows.Next() {
		var id string
		var votes int64
		if err := rows.Scan(&id, &votes); err != nil {
			return nil, false
		}
		counts = append(counts, map[string]any{"option_id": id, "votes": votes})
	}
	return map[string]any{
		"event_id": eventID, "author": author, "question": question, "poll_type": pollType,
		"options": jsonRawOrEmpty(options), "counts": counts, "consistency": "eventual",
	}, true
}
