package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

func (h Handlers) GetClientStats(w http.ResponseWriter, r *http.Request) {
	days, ok := statsWindowDays(w, r)
	if !ok {
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT client, COALESCE(sum(event_count), 0)::bigint
		FROM client_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - $1::int
		GROUP BY client
		ORDER BY sum(event_count) DESC, client ASC
		LIMIT 50
	`, days)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	clients := make([]map[string]any, 0)
	for rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		authors, _ := h.countClientAuthors(r.Context(), name, days)
		clients = append(clients, map[string]any{
			"client":         name,
			"event_count":    count,
			"unique_authors": authors,
		})
	}
	flags := h.contentFlagTotals(r.Context(), days)
	writeJSON(w, http.StatusOK, map[string]any{
		"window":        statsWindowLabel(days),
		"clients":       clients,
		"content_flags": flags,
	})
}

func (h Handlers) countClientAuthors(ctx context.Context, client string, days int) (int64, error) {
	if client == "(none)" {
		return 0, nil
	}
	var n int64
	err := h.pool.QueryRow(ctx, `
		SELECT count(DISTINCT pubkey)::bigint FROM client_author_daily
		WHERE client = $1 AND day >= (now() AT TIME ZONE 'utc')::date - $2::int
	`, client, days).Scan(&n)
	return n, err
}

func (h Handlers) contentFlagTotals(ctx context.Context, days int) map[string]any {
	var total, bridged, warning, pow int64
	_ = h.pool.QueryRow(ctx, `
		SELECT COALESCE(sum(total_count),0), COALESCE(sum(bridged_count),0),
		       COALESCE(sum(content_warning_count),0), COALESCE(sum(pow_count),0)
		FROM content_flags_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - $1::int
	`, days).Scan(&total, &bridged, &warning, &pow)
	share := func(part int64) float64 {
		if total == 0 {
			return 0
		}
		return float64(part) / float64(total)
	}
	return map[string]any{
		"events":                total,
		"bridged_share":         share(bridged),
		"content_warning_share": share(warning),
		"pow_share":             share(pow),
	}
}

func (h Handlers) GetIdentityStats(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	var profiles, withNIP05, verified, withLud16, withLud06, bots int64
	var avgCompleteness float64
	err := h.pool.QueryRow(r.Context(), `
		SELECT
			count(*)::bigint,
			count(*) FILTER (WHERE f.nip05 <> '')::bigint,
			count(*) FILTER (WHERE v.status = 'verified')::bigint,
			count(*) FILTER (WHERE f.lud16 <> '')::bigint,
			count(*) FILTER (WHERE f.has_lud06)::bigint,
			count(*) FILTER (WHERE f.is_bot)::bigint,
			COALESCE(avg(f.completeness_score), 0)
		FROM profile_fields f
		LEFT JOIN nip05_verifications v ON v.pubkey = f.pubkey
	`).Scan(&profiles, &withNIP05, &verified, &withLud16, &withLud06, &bots, &avgCompleteness)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"profiles":         profiles,
		"nip05_present":    withNIP05,
		"nip05_verified":   verified,
		"lud16_present":    withLud16,
		"lud06_present":    withLud06,
		"bot_flagged":      bots,
		"avg_completeness": avgCompleteness,
		"nip05_domains":    h.topDomains(r.Context(), "nip05_domain"),
		"lud16_domains":    h.topDomains(r.Context(), "lud16_domain"),
	})
}

func (h Handlers) topDomains(ctx context.Context, column string) []map[string]any {
	if column != "nip05_domain" && column != "lud16_domain" {
		return nil
	}
	rows, err := h.pool.Query(ctx, `
		SELECT `+column+`, count(*)::bigint
		FROM profile_fields
		WHERE `+column+` <> ''
		GROUP BY `+column+`
		ORDER BY count(*) DESC, `+column+` ASC
		LIMIT 20
	`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var domain string
		var count int64
		if err := rows.Scan(&domain, &count); err != nil {
			return out
		}
		out = append(out, map[string]any{"domain": domain, "count": count})
	}
	return out
}

func (h Handlers) GetZapStats(w http.ResponseWriter, r *http.Request) {
	days, ok := statsWindowDays(w, r)
	if !ok || h.pool == nil {
		if h.pool == nil && ok {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		}
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT day, zap_count, sats_total, self_zap_count, nutzap_count, nutzap_sats, hist
		FROM zap_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - $1::int
		ORDER BY day ASC
	`, days)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	series := make([]map[string]any, 0)
	for rows.Next() {
		var day time.Time
		var zapCount, sats, self, nutzapCount, nutzapSats int64
		var hist []byte
		if err := rows.Scan(&day, &zapCount, &sats, &self, &nutzapCount, &nutzapSats, &hist); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		series = append(series, map[string]any{
			"day": day.Format("2006-01-02"), "zap_count": zapCount, "sats_total": sats,
			"self_zap_count": self, "nutzap_count": nutzapCount, "nutzap_sats": nutzapSats, "histogram": jsonRawOrEmpty(hist),
		})
	}
	var senders, receivers int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT
			count(DISTINCT pubkey) FILTER (WHERE role = 'sender')::bigint,
			count(DISTINCT pubkey) FILTER (WHERE role = 'receiver')::bigint
		FROM zap_participant_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - $1::int
	`, days).Scan(&senders, &receivers)
	writeJSON(w, http.StatusOK, map[string]any{
		"window": statsWindowLabel(days), "series": series,
		"unique_senders": senders, "unique_receivers": receivers,
	})
}

// accountAgeEpochFloor is 2020-01-01 UTC. A stored created_at older than
// this is not an account age: Nostr did not exist yet.
const accountAgeEpochFloor int64 = 1577836800

func (h Handlers) GetAccountStats(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	policy := h.newPublicCachePolicy(publicCacheFamilyStats, "account_stats", nil)
	if err := h.servePublicCached(r.Context(), w, policy, h.accountStatsPayload); err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func (h Handlers) accountStatsPayload(ctx context.Context) (map[string]any, error) {
	var (
		newAuthors               []map[string]any
		dau, wau, active30       int64
		profiles, postedProfiles int64
		losses, gains            int64
		ageCounts                map[int]int64
		aged                     int64
	)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rows, err := h.dailyCounts(ctx, `
			SELECT (first_seen_at AT TIME ZONE 'utc')::date, count(*)::bigint
			FROM author_first_seen
			WHERE first_seen_at >= now() - interval '30 days'
			GROUP BY 1 ORDER BY 1
		`)
		if err != nil {
			return err
		}
		newAuthors = rows
		return nil
	})
	g.Go(func() error {
		return h.pool.QueryRow(ctx, `
			SELECT
				count(DISTINCT pubkey) FILTER (WHERE activity_date >= (now() AT TIME ZONE 'utc')::date - 1),
				count(DISTINCT pubkey) FILTER (WHERE activity_date >= (now() AT TIME ZONE 'utc')::date - 7),
				count(DISTINCT pubkey)
			FROM author_activity_daily
			WHERE activity_date >= (now() AT TIME ZONE 'utc')::date - 30
			  AND post_count > 0
		`).Scan(&dau, &wau, &active30)
	})
	// profiles is the kind-0 population, the same count as identity stats.
	// Dormant profiles are that population minus those who posted in 30 days,
	// so the two numbers share a denominator. active_authors_* stays every
	// pubkey who posted, including pubkeys that never published a profile.
	//
	// Posters are aggregated once from the partial activity_date index, then
	// hashed against profile_fields. A per-profile EXISTS was planned as a
	// nested index lookup, one probe per profile, which is what pushed this
	// route past the client timeout.
	g.Go(func() error {
		return h.pool.QueryRow(ctx, `
			WITH posters AS MATERIALIZED (
				SELECT DISTINCT pubkey
				FROM author_activity_daily
				WHERE activity_date >= (now() AT TIME ZONE 'utc')::date - 30
				  AND post_count > 0
			)
			SELECT
				count(*)::bigint,
				count(posters.pubkey)::bigint
			FROM profile_fields
			LEFT JOIN posters ON posters.pubkey = profile_fields.pubkey
		`).Scan(&profiles, &postedProfiles)
	})
	g.Go(func() error {
		return h.pool.QueryRow(ctx, `
			SELECT
				(SELECT count(*) FROM follower_loss_events WHERE lost_at >= EXTRACT(EPOCH FROM now())::bigint - 86400 * 7),
				(SELECT count(*) FROM follower_gain_events WHERE gained_at >= EXTRACT(EPOCH FROM now())::bigint - 86400 * 7)
		`).Scan(&losses, &gains)
	})
	// Age is the oldest stored event for a kind-0 profile, not the moment
	// the index first saw the pubkey. Future dates and pre-Nostr dates are
	// counted separately so they do not land in the newest or oldest bucket.
	g.Go(func() error {
		rows, err := h.pool.Query(ctx, `
			SELECT width_bucket(
			         ((EXTRACT(EPOCH FROM now()) - s.time_joined) / 86400.0)::float8,
			         ARRAY[1, 7, 30, 90, 365]::float8[]
			       ),
			       count(*)::bigint
			FROM profile_fields f
			JOIN profile_public_stats s ON s.pubkey = f.pubkey
			WHERE s.time_joined IS NOT NULL
			  AND s.time_joined >= $1
			  AND s.time_joined <= EXTRACT(EPOCH FROM now())::bigint
			GROUP BY 1
		`, accountAgeEpochFloor)
		if err != nil {
			return err
		}
		defer rows.Close()
		counts := map[int]int64{}
		var known int64
		for rows.Next() {
			var bucket int
			var count int64
			if err := rows.Scan(&bucket, &count); err != nil {
				return err
			}
			if bucket < 0 || bucket > 5 {
				continue
			}
			counts[bucket] = count
			known += count
		}
		if err := rows.Err(); err != nil {
			return err
		}
		ageCounts = counts
		aged = known
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	unfollow := int64(0)
	if losses+gains > 0 {
		unfollow = losses * 10000 / (losses + gains)
	}
	return map[string]any{
		"new_authors":         newAuthors,
		"active_authors_1d":   dau,
		"active_authors_7d":   wau,
		"active_authors_30d":  active30,
		"profiles":            profiles,
		"profiles_posted_30d": postedProfiles,
		"dormant_30d":         profiles - postedProfiles,
		"unfollow_rate_bps":   unfollow,
		"account_age":         accountAgeBuckets(ageCounts),
		"account_age_unknown": profiles - aged,
	}, nil
}

// accountAgeBuckets returns one row per width_bucket index. Thresholds are
// 1, 7, 30, 90, and 365 days: 0 is under a day, 5 is a year or more.
func accountAgeBuckets(counts map[int]int64) []map[string]any {
	out := make([]map[string]any, 0, 6)
	for bucket := 0; bucket < 6; bucket++ {
		out = append(out, map[string]any{"bucket": bucket, "authors": counts[bucket]})
	}
	return out
}

func (h Handlers) GetRelayPropagation(w http.ResponseWriter, r *http.Request) {
	days, ok := statsWindowDays(w, r)
	if !ok || h.pool == nil {
		if h.pool == nil && ok {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		}
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		SELECT relay_url,
		       sum(events_seen)::bigint,
		       sum(first_seen_count)::bigint,
		       sum(exclusive_count)::bigint,
		       sum(future_dated_count)::bigint,
		       COALESCE(avg(latency_p50_ms), 0),
		       COALESCE(avg(untrusted_event_share), 0)
		FROM relay_propagation_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - $1::int
		GROUP BY relay_url
		ORDER BY sum(events_seen) DESC
		LIMIT 50
	`, days)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	relays := make([]map[string]any, 0)
	for rows.Next() {
		var url string
		var seen, first, exclusive, future int64
		var latency, untrusted float64
		if err := rows.Scan(&url, &seen, &first, &exclusive, &future, &latency, &untrusted); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		share := func(part int64) float64 {
			if seen == 0 {
				return 0
			}
			return float64(part) / float64(seen)
		}
		relays = append(relays, map[string]any{
			"relay_url": url, "events_seen": seen,
			"first_seen_share": share(first), "exclusive_share": share(exclusive),
			"future_dated_share": share(future),
			"latency_p50_ms":     latency, "untrusted_share": untrusted,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"window": statsWindowLabel(days), "relays": relays})
}

func (h Handlers) GetRelayOverlap(w http.ResponseWriter, r *http.Request) {
	days, ok := statsWindowDays(w, r)
	if !ok || h.pool == nil {
		if h.pool == nil && ok {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		}
		return
	}
	rows, err := h.pool.Query(r.Context(), `
		WITH overlap AS (
			SELECT relay_a, relay_b, sum(shared_events)::bigint AS shared
			FROM relay_overlap_daily
			WHERE day >= (now() AT TIME ZONE 'utc')::date - $1::int
			GROUP BY relay_a, relay_b
		),
		seen AS (
			SELECT relay_url, sum(events_seen)::bigint AS events_seen
			FROM relay_propagation_daily
			WHERE day >= (now() AT TIME ZONE 'utc')::date - $1::int
			GROUP BY relay_url
		)
		SELECT o.relay_a, o.relay_b, o.shared,
		       COALESCE(sa.events_seen, 0), COALESCE(sb.events_seen, 0)
		FROM overlap o
		LEFT JOIN seen sa ON sa.relay_url = o.relay_a
		LEFT JOIN seen sb ON sb.relay_url = o.relay_b
		ORDER BY o.shared DESC
		LIMIT 200
	`, days)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer rows.Close()
	pairs := make([]map[string]any, 0)
	for rows.Next() {
		var a, b string
		var shared, seenA, seenB int64
		if err := rows.Scan(&a, &b, &shared, &seenA, &seenB); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		pair := map[string]any{"relay_a": a, "relay_b": b, "shared_events": shared}
		denom := seenA + seenB - shared
		if denom > 0 && shared >= 0 {
			pair["jaccard"] = float64(shared) / float64(denom)
		}
		pairs = append(pairs, pair)
	}
	writeJSON(w, http.StatusOK, map[string]any{"window": statsWindowLabel(days), "pairs": pairs})
}

func (h Handlers) GetAuthorZapAnalytics(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if len(pubkey) != 64 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	days, ok := statsWindowDays(w, r)
	if !ok || h.pool == nil {
		if h.pool == nil && ok {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		}
		return
	}
	series, err := h.authorZapSeries(r.Context(), pubkey, days)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	var received, sent, distinct int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT COALESCE(sum(zap_count),0)::bigint, COALESCE(sum(sats_total),0)::bigint
		FROM zap_edges WHERE receiver_pubkey = $1
	`, pubkey).Scan(&received, &sent)
	_ = h.pool.QueryRow(r.Context(), `
		SELECT count(*)::bigint FROM zap_edges WHERE receiver_pubkey = $1 AND zap_count > 1
	`, pubkey).Scan(&distinct)
	var followers, reactions int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT COALESCE(follower_count, 0) FROM profile_public_stats WHERE pubkey = $1
	`, pubkey).Scan(&followers)
	_ = h.pool.QueryRow(r.Context(), `
		SELECT COALESCE(sum(plus_count + minus_count + emoji_count), 0)::bigint
		FROM author_reaction_received_daily
		WHERE pubkey = $1 AND day >= (now() AT TIME ZONE 'utc')::date - $2::int
	`, pubkey, days).Scan(&reactions)
	satsPer1k := int64(0)
	if followers > 0 {
		satsPer1k = sent * 1000 / followers
	}
	zapReaction := float64(0)
	if reactions > 0 {
		zapReaction = float64(received) / float64(reactions)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey": pubkey, "window": statsWindowLabel(days), "series": series,
		"inbound_zaps": received, "inbound_sats": sent, "repeat_zappers": distinct,
		"sats_per_1k_followers": satsPer1k, "zap_to_reaction_ratio": zapReaction,
	})
}

func (h Handlers) authorZapSeries(ctx context.Context, pubkey string, days int) ([]map[string]any, error) {
	rows, err := h.pool.Query(ctx, `
		SELECT day, received_count, received_sats, sent_count, sent_sats, nutzap_received_count, nutzap_received_sats
		FROM profile_zap_daily
		WHERE pubkey = $1 AND day >= (now() AT TIME ZONE 'utc')::date - $2::int
		ORDER BY day ASC
	`, pubkey, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var day time.Time
		var rc, rs, sc, ss, nc, ns int64
		if err := rows.Scan(&day, &rc, &rs, &sc, &ss, &nc, &ns); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"day":            day.Format("2006-01-02"),
			"received_count": rc, "received_sats": rs,
			"sent_count": sc, "sent_sats": ss,
			"nutzap_received_count": nc, "nutzap_received_sats": ns,
		})
	}
	return out, rows.Err()
}

func (h Handlers) GetProfileIdentity(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if len(pubkey) != 64 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "feature_unavailable", "stats database is not configured")
		return
	}
	var name, display, nip05ID, domain, lud16, lud16Domain, website, status string
	var picture, banner, about, bot, lud06 bool
	var score int
	var checkedAt *time.Time
	err := h.pool.QueryRow(r.Context(), `
		SELECT f.name, f.display_name, f.nip05, f.nip05_domain, f.lud16, f.lud16_domain, f.website_domain,
		       f.has_picture, f.has_banner, f.has_about, f.is_bot, f.has_lud06, f.completeness_score,
		       COALESCE(v.status, ''), v.checked_at
		FROM profile_fields f
		LEFT JOIN nip05_verifications v ON v.pubkey = f.pubkey
		WHERE f.pubkey = $1
	`, pubkey).Scan(&name, &display, &nip05ID, &domain, &lud16, &lud16Domain, &website,
		&picture, &banner, &about, &bot, &lud06, &score, &status, &checkedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "profile identity is not projected")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	var acceptsNutzap bool
	_ = h.pool.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM nutzap_info_latest WHERE pubkey = $1)`, pubkey).Scan(&acceptsNutzap)
	payload := map[string]any{
		"pubkey": pubkey, "name": name, "display_name": display,
		"nip05": nip05ID, "nip05_domain": domain, "nip05_verified": status == "verified",
		"nip05_status": status, "lud16": lud16, "lud16_domain": lud16Domain,
		"website_domain": website, "has_picture": picture, "has_banner": banner,
		"has_about": about, "is_bot": bot, "has_lud06": lud06,
		"completeness_score": score, "accepts_nutzaps": acceptsNutzap,
	}
	if checkedAt != nil {
		payload["nip05_checked_at"] = checkedAt.Unix()
	}
	var activeStatuses int64
	_ = h.pool.QueryRow(r.Context(), `
		SELECT count(*)::bigint FROM user_status_latest
		WHERE pubkey = $1 AND (expires_at IS NULL OR expires_at > EXTRACT(EPOCH FROM now())::bigint)
	`, pubkey).Scan(&activeStatuses)
	payload["active_statuses"] = activeStatuses
	payload["graph"] = h.profileGraphPayload(r.Context(), pubkey)
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) profileGraphPayload(ctx context.Context, pubkey string) map[string]any {
	out := map[string]any{}
	if h.pool == nil {
		return out
	}
	var mutual, reciprocity, followBack, ghost, mentioned int64
	if err := h.pool.QueryRow(ctx, `
		SELECT mutual_count, reciprocity_bps, follow_back_rate_bps, ghost_follower_bps, mentioned_not_followed_count
		FROM profile_graph_stats WHERE pubkey = $1
	`, pubkey).Scan(&mutual, &reciprocity, &followBack, &ghost, &mentioned); err != nil {
		return out
	}
	return map[string]any{
		"mutual_count": mutual, "reciprocity_bps": reciprocity,
		"follow_back_rate_bps": followBack, "ghost_follower_bps": ghost,
		"mentioned_not_followed_count": mentioned,
	}
}

func (h Handlers) dailyCounts(ctx context.Context, sql string) ([]map[string]any, error) {
	rows, err := h.pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var day time.Time
		var count int64
		if err := rows.Scan(&day, &count); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"day": day.Format("2006-01-02"), "count": count})
	}
	return out, rows.Err()
}

func statsWindowDays(w http.ResponseWriter, r *http.Request) (int, bool) {
	switch strings.TrimSpace(r.URL.Query().Get("window")) {
	case "", "7d":
		return 7, true
	case "24h":
		return 1, true
	case "30d":
		return 30, true
	default:
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "window must be one of 24h, 7d, 30d")
		return 0, false
	}
}

func statsWindowLabel(days int) string {
	switch days {
	case 1:
		return "24h"
	case 30:
		return "30d"
	default:
		return "7d"
	}
}

func jsonRawOrEmpty(raw []byte) any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	return jsonRaw(raw)
}

type jsonRaw []byte

func (j jsonRaw) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return []byte(`{}`), nil
	}
	return j, nil
}
