package derivation

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func refreshAnalyticsRollupTx(ctx context.Context, tx pgx.Tx, day string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE zap_daily d
		SET amount_p50_sats = stats.p50,
		    amount_p90_sats = stats.p90,
		    amount_mean_sats = stats.mean,
		    gini_bps = stats.gini,
		    ttfz_p50_s = stats.ttfz
		FROM (
			SELECT
				COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY amount_sats), 0)::bigint AS p50,
				COALESCE(percentile_cont(0.9) WITHIN GROUP (ORDER BY amount_sats), 0)::bigint AS p90,
				COALESCE(avg(amount_sats), 0)::bigint AS mean,
				COALESCE((
					SELECT CASE
						WHEN count(*) = 0 OR sum(amount_sats) = 0 THEN 0
						ELSE GREATEST(0, (
							(2.0 * sum(rank_index * amount_sats)) / (count(*) * sum(amount_sats))
							- (count(*) + 1.0) / count(*)
						) * 10000)
					END
					FROM (
						SELECT amount_sats, row_number() OVER (ORDER BY amount_sats) AS rank_index
						FROM zap_receipts
						WHERE created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
						  AND created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
						  AND amount_sats > 0
					) ranked
				), 0)::bigint AS gini,
				COALESCE((
					SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY first_zap_at - created_at)
					FROM note_discovery_stats
					WHERE first_zap_at IS NOT NULL
					  AND created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
					  AND created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
					  AND first_zap_at >= created_at
				), 0)::bigint AS ttfz
			FROM zap_receipts
			WHERE created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
			  AND created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
			  AND amount_sats > 0
		) stats
		WHERE d.day = $1::date
	`, day); err != nil {
		return fmt.Errorf("rollup zap percentiles: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO zap_leaderboard_daily (day, role, pubkey, sats, zap_count)
		SELECT $1::date, role, pubkey, sats, zap_count
		FROM (
			SELECT 'sender' AS role, sender_pubkey AS pubkey, sum(amount_sats)::bigint AS sats, count(*)::bigint AS zap_count
			FROM zap_receipts
			WHERE created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
			  AND created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
			  AND sender_pubkey <> ''
			GROUP BY sender_pubkey
			ORDER BY sum(amount_sats) DESC
			LIMIT 100
		) senders
		UNION ALL
		SELECT $1::date, role, pubkey, sats, zap_count
		FROM (
			SELECT 'receiver' AS role, receiver_pubkey AS pubkey, sum(amount_sats)::bigint AS sats, count(*)::bigint AS zap_count
			FROM zap_receipts
			WHERE created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
			  AND created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
			  AND receiver_pubkey <> ''
			GROUP BY receiver_pubkey
			ORDER BY sum(amount_sats) DESC
			LIMIT 100
		) receivers
		ON CONFLICT (day, role, pubkey) DO UPDATE
		SET sats = EXCLUDED.sats, zap_count = EXCLUDED.zap_count
	`, day); err != nil {
		return fmt.Errorf("rollup zap leaderboard: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO zap_hourly (day_of_week, hour, zap_count, sats)
		SELECT EXTRACT(DOW FROM to_timestamp(created_at) AT TIME ZONE 'utc')::int,
		       EXTRACT(HOUR FROM to_timestamp(created_at) AT TIME ZONE 'utc')::int,
		       count(*)::bigint,
		       COALESCE(sum(amount_sats), 0)::bigint
		FROM zap_receipts
		WHERE created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
		  AND created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
		GROUP BY 1, 2
		ON CONFLICT (day_of_week, hour) DO UPDATE
		SET zap_count = zap_hourly.zap_count + EXCLUDED.zap_count,
		    sats = zap_hourly.sats + EXCLUDED.sats
	`, day); err != nil {
		return fmt.Errorf("rollup zap hourly: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO content_shape_daily (day, kind, length_hist, mention_total, hashtag_total, posts)
		SELECT $1::date, e.kind,
		       jsonb_build_object(
		           '0_80', count(*) FILTER (WHERE a.content_length < 80),
		           '80_280', count(*) FILTER (WHERE a.content_length BETWEEN 80 AND 279),
		           '280_1000', count(*) FILTER (WHERE a.content_length BETWEEN 280 AND 999),
		           '1000_plus', count(*) FILTER (WHERE a.content_length >= 1000)
		       ),
		       COALESCE(sum(a.mention_count), 0)::bigint,
		       COALESCE(sum(a.hashtag_count), 0)::bigint,
		       count(*)::bigint
		FROM event_attributes a
		JOIN events e ON e.id = a.event_id
		WHERE e.created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
		  AND e.created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
		GROUP BY e.kind
		ON CONFLICT (day, kind) DO UPDATE
		SET length_hist = EXCLUDED.length_hist,
		    mention_total = EXCLUDED.mention_total,
		    hashtag_total = EXCLUDED.hashtag_total,
		    posts = EXCLUDED.posts
	`, day); err != nil {
		return fmt.Errorf("rollup content shape: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO hashtag_daily (day, hashtag, events, authors)
		SELECT $1::date, hashtag, count(*)::bigint, count(DISTINCT author_pubkey)::bigint
		FROM event_hashtags
		WHERE created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
		  AND created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
		GROUP BY hashtag
		ON CONFLICT (day, hashtag) DO UPDATE
		SET events = EXCLUDED.events, authors = EXCLUDED.authors
	`, day); err != nil {
		return fmt.Errorf("rollup hashtags: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO hashtag_momentum (hashtag, z_score_7d, status, half_life_days, updated_at)
		SELECT hashtag,
		       CASE WHEN prior = 0 THEN recent ELSE (recent - prior)::float8 / GREATEST(prior, 1) END,
		       CASE
		           WHEN recent >= prior * 2 AND recent >= 20 THEN 'emerging'
		           WHEN prior > 0 AND recent * 2 < prior THEN 'fading'
		           ELSE 'steady'
		       END,
		       0,
		       now()
		FROM (
			SELECT hashtag,
			       COALESCE(sum(events) FILTER (WHERE day >= $1::date - 6), 0) AS recent,
			       COALESCE(sum(events) FILTER (WHERE day < $1::date - 6 AND day >= $1::date - 13), 0) AS prior
			FROM hashtag_daily
			GROUP BY hashtag
			HAVING sum(events) >= 20
		) windows
		ON CONFLICT (hashtag) DO UPDATE
		SET z_score_7d = EXCLUDED.z_score_7d,
		    status = EXCLUDED.status,
		    updated_at = now()
	`, day); err != nil {
		return fmt.Errorf("rollup hashtag momentum: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO kind_engagement_daily (day, kind, posts, reactions, replies, zaps)
		SELECT $1::date, e.kind, count(*)::bigint,
		       COALESCE(sum(n.reaction_count), 0)::bigint,
		       COALESCE(sum(n.reply_count), 0)::bigint,
		       COALESCE(sum(n.zap_count), 0)::bigint
		FROM note_discovery_stats n
		JOIN events e ON e.id = n.event_id
		WHERE n.created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
		  AND n.created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
		GROUP BY e.kind
		ON CONFLICT (day, kind) DO UPDATE
		SET posts = EXCLUDED.posts, reactions = EXCLUDED.reactions,
		    replies = EXCLUDED.replies, zaps = EXCLUDED.zaps
	`, day); err != nil {
		return fmt.Errorf("rollup kind engagement: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO author_cohort_retention (cohort_week, week_offset, active_authors)
		SELECT date_trunc('week', f.first_seen_at)::date,
		       ((a.activity_date - date_trunc('week', f.first_seen_at)::date) / 7)::int,
		       count(DISTINCT a.pubkey)::bigint
		FROM author_activity_daily a
		JOIN author_first_seen f ON f.pubkey = a.pubkey
		WHERE a.activity_date = $1::date
		  AND a.post_count > 0
		GROUP BY 1, 2
		ON CONFLICT (cohort_week, week_offset) DO UPDATE
		SET active_authors = EXCLUDED.active_authors
	`, day); err != nil {
		return fmt.Errorf("rollup cohort retention: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO relay_list_stats_daily (day, lists, avg_relays_x100, p50_relays, read_only_share_bps, write_only_share_bps)
		SELECT $1::date,
		       count(*)::bigint,
		       COALESCE(avg(CASE WHEN jsonb_typeof(relays_json) = 'array' THEN jsonb_array_length(relays_json) ELSE 0 END) * 100, 0)::bigint,
		       COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY CASE WHEN jsonb_typeof(relays_json) = 'array' THEN jsonb_array_length(relays_json) ELSE 0 END), 0)::bigint,
		       0,
		       0
		FROM relay_lists_latest
		ON CONFLICT (day) DO UPDATE
		SET lists = EXCLUDED.lists, avg_relays_x100 = EXCLUDED.avg_relays_x100, p50_relays = EXCLUDED.p50_relays
	`, day); err != nil {
		return fmt.Errorf("rollup relay lists: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO relay_health_daily (day, url_key, probes, connect_ok_bps, connect_p50_ms, connect_p90_ms, eose_p50_ms)
		SELECT $1::date, url_key,
		       count(*)::bigint,
		       (count(*) FILTER (WHERE connect_ok) * 10000 / GREATEST(count(*), 1))::bigint,
		       COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY connect_latency_ms), 0)::bigint,
		       COALESCE(percentile_cont(0.9) WITHIN GROUP (ORDER BY connect_latency_ms), 0)::bigint,
		       COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY eose_latency_ms), 0)::bigint
		FROM relay_probe_observations
		WHERE probed_at >= $1::date AND probed_at < $1::date + interval '1 day'
		GROUP BY url_key
		ON CONFLICT (day, url_key) DO UPDATE
		SET probes = EXCLUDED.probes, connect_ok_bps = EXCLUDED.connect_ok_bps,
		    connect_p50_ms = EXCLUDED.connect_p50_ms, connect_p90_ms = EXCLUDED.connect_p90_ms,
		    eose_p50_ms = EXCLUDED.eose_p50_ms
	`, day); err != nil {
		return fmt.Errorf("rollup relay health: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO network_graph_daily (day, follower_degree_histogram, mutual_edge_share_bps)
		SELECT $1::date,
		       jsonb_build_object(
		           '0', count(*) FILTER (WHERE follower_count = 0),
		           '1_10', count(*) FILTER (WHERE follower_count BETWEEN 1 AND 10),
		           '11_100', count(*) FILTER (WHERE follower_count BETWEEN 11 AND 100),
		           '101_1000', count(*) FILTER (WHERE follower_count BETWEEN 101 AND 1000),
		           '1000_plus', count(*) FILTER (WHERE follower_count > 1000)
		       ),
		       0
		FROM profile_public_stats
		ON CONFLICT (day) DO UPDATE
		SET follower_degree_histogram = EXCLUDED.follower_degree_histogram
	`, day); err != nil {
		return fmt.Errorf("rollup network graph: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO thread_shape_daily (day, threads, p50_first_reply_s, avg_depth_x100, avg_branching_x100)
		SELECT $1::date, count(*)::bigint,
		       COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY first_reply_latency_s) FILTER (WHERE first_reply_latency_s > 0), 0)::bigint,
		       COALESCE(avg(max_depth) * 100, 0)::bigint,
		       COALESCE(avg(branching_x100), 0)::bigint
		FROM thread_summaries
		WHERE projected_at >= $1::date AND projected_at < $1::date + interval '1 day'
		ON CONFLICT (day) DO UPDATE
		SET threads = EXCLUDED.threads, p50_first_reply_s = EXCLUDED.p50_first_reply_s,
		    avg_depth_x100 = EXCLUDED.avg_depth_x100, avg_branching_x100 = EXCLUDED.avg_branching_x100
	`, day); err != nil {
		return fmt.Errorf("rollup thread shape: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM follower_loss_events WHERE created_at < now() - interval '35 days'`); err != nil {
		return fmt.Errorf("prune follower losses: %w", err)
	}
	return nil
}
