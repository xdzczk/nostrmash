package derivation

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RefreshEcosystemDailyRollup writes yesterday's relay propagation and
// overlap rows once. It scans only that UTC day of event_relays, and it
// does not rewrite a day that already has a marker.
func (h *Handlers) RefreshEcosystemDailyRollup(ctx context.Context) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	day := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	var exists bool
	if err := h.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ecosystem_daily_rollup WHERE day = $1::date)`, day).Scan(&exists); err != nil {
		return fmt.Errorf("check ecosystem rollup marker: %w", err)
	}
	if exists {
		return nil
	}
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '90s'`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO relay_propagation_daily (
				day, relay_url, events_seen, first_seen_count, exclusive_count,
				latency_p50_ms, latency_p90_ms, future_dated_count, untrusted_event_share, kind_mix
			)
			SELECT
				$1::date,
				er.relay_url,
				count(*)::bigint,
				count(*) FILTER (WHERE er.is_first_seen)::bigint,
				count(*) FILTER (WHERE per_event.relay_n = 1)::bigint,
				COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY lat.latency_ms) FILTER (WHERE lat.latency_ms BETWEEN 0 AND 86400000), 0),
				COALESCE(percentile_cont(0.9) WITHIN GROUP (ORDER BY lat.latency_ms) FILTER (WHERE lat.latency_ms BETWEEN 0 AND 86400000), 0),
				count(*) FILTER (WHERE lat.latency_ms < 0)::bigint,
				CASE
					WHEN NOT EXISTS (SELECT 1 FROM trust_graph_snapshot) THEN NULL
					ELSE count(*) FILTER (WHERE ts.pubkey IS NULL)::double precision / NULLIF(count(*), 0)
				END,
				COALESCE(kind_mix.mix, '{}'::jsonb)
			FROM event_relays er
			JOIN events e ON e.id = er.event_id
			JOIN (
				SELECT event_id, count(*)::bigint AS relay_n
				FROM event_relays
				WHERE seen_at >= $1::date AND seen_at < $1::date + interval '1 day'
				GROUP BY event_id
			) per_event ON per_event.event_id = er.event_id
			CROSS JOIN LATERAL (
				SELECT (EXTRACT(EPOCH FROM (er.seen_at - to_timestamp(e.created_at))) * 1000)::double precision AS latency_ms
			) lat
			LEFT JOIN trust_graph_snapshot ts ON ts.pubkey = e.pubkey
			LEFT JOIN (
				SELECT per_kind.relay_url, jsonb_object_agg(per_kind.kind::text, per_kind.n) AS mix
				FROM (
					SELECT er2.relay_url, e2.kind, count(*)::bigint AS n
					FROM event_relays er2
					JOIN events e2 ON e2.id = er2.event_id
					WHERE er2.seen_at >= $1::date
					  AND er2.seen_at < $1::date + interval '1 day'
					GROUP BY er2.relay_url, e2.kind
				) per_kind
				GROUP BY per_kind.relay_url
			) kind_mix ON kind_mix.relay_url = er.relay_url
			WHERE er.seen_at >= $1::date AND er.seen_at < $1::date + interval '1 day'
			GROUP BY er.relay_url, kind_mix.mix
			ON CONFLICT (day, relay_url) DO UPDATE
			SET events_seen = EXCLUDED.events_seen,
			    first_seen_count = EXCLUDED.first_seen_count,
			    exclusive_count = EXCLUDED.exclusive_count,
			    latency_p50_ms = EXCLUDED.latency_p50_ms,
			    latency_p90_ms = EXCLUDED.latency_p90_ms,
			    future_dated_count = EXCLUDED.future_dated_count,
			    untrusted_event_share = EXCLUDED.untrusted_event_share,
			    kind_mix = EXCLUDED.kind_mix
		`, day); err != nil {
			return fmt.Errorf("rollup relay propagation: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO relay_overlap_daily (day, relay_a, relay_b, shared_events)
			SELECT $1::date, a.relay_url, b.relay_url, count(*)::bigint
			FROM event_relays a
			JOIN event_relays b
			  ON b.event_id = a.event_id
			 AND b.relay_url > a.relay_url
			WHERE a.seen_at >= $1::date
			  AND a.seen_at < $1::date + interval '1 day'
			  AND b.seen_at >= $1::date
			  AND b.seen_at < $1::date + interval '1 day'
			  AND EXISTS (
			      SELECT 1 FROM relay_registry ra
			      WHERE ra.normalized_url = a.relay_url
			        AND ra.admission_state IN ('active', 'pinned', 'probation')
			  )
			  AND EXISTS (
			      SELECT 1 FROM relay_registry rb
			      WHERE rb.normalized_url = b.relay_url
			        AND rb.admission_state IN ('active', 'pinned', 'probation')
			  )
			GROUP BY a.relay_url, b.relay_url
			ON CONFLICT (day, relay_a, relay_b) DO UPDATE
			SET shared_events = EXCLUDED.shared_events
		`, day); err != nil {
			return fmt.Errorf("rollup relay overlap: %w", err)
		}
		if err := refreshAnalyticsRollupTx(ctx, tx, day); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO ecosystem_daily_rollup (day) VALUES ($1::date)
			ON CONFLICT (day) DO NOTHING
		`, day); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM group_author_daily WHERE day < (now() AT TIME ZONE 'utc')::date - 35`); err != nil {
			return fmt.Errorf("prune group authors: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM live_chat_author_daily WHERE day < (now() AT TIME ZONE 'utc')::date - 35`); err != nil {
			return fmt.Errorf("prune live chat authors: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM groups_latest g
			WHERE g.updated_at < now() - interval '60 days'
			  AND NOT EXISTS (
			      SELECT 1 FROM group_messages m
			      WHERE m.relay_url = g.relay_url AND m.group_id = g.group_id
			        AND m.created_at >= extract(epoch FROM now() - interval '60 days')::bigint
			  )
		`); err != nil {
			return fmt.Errorf("prune idle groups: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM kind_census_events WHERE seen_day < (now() AT TIME ZONE 'utc')::date - 35`); err != nil {
			return fmt.Errorf("prune kind census: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM kind_author_daily WHERE day < (now() AT TIME ZONE 'utc')::date - 35`); err != nil {
			return fmt.Errorf("prune kind authors: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM client_author_daily WHERE day < (now() AT TIME ZONE 'utc')::date - 35`); err != nil {
			return fmt.Errorf("prune client authors: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM zap_participant_daily WHERE day < (now() AT TIME ZONE 'utc')::date - 35`); err != nil {
			return fmt.Errorf("prune zap participants: %w", err)
		}
		return nil
	})
}
