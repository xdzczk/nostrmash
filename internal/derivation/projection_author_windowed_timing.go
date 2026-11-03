package derivation

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) upsertAuthorActivityWindowsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM author_activity_windows
		WHERE pubkey = $1
		  AND window_days = $2
	`, pubkey, windowDays); err != nil {
		return fmt.Errorf("delete prior author activity windows for %s window=%dd: %w", pubkey, windowDays, err)
	}

	if h.incrementalWindowedRollups {
		var hasDaily bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM author_hourly_activity
				WHERE pubkey = $1
				  AND activity_date >= to_timestamp($2)::date
				  AND engagement_received > 0
			)
		`, pubkey, cutoff).Scan(&hasDaily); err != nil {
			return fmt.Errorf("check author_hourly_activity engagement for %s: %w", pubkey, err)
		}
		if hasDaily {
			_, err := tx.Exec(ctx, `
				INSERT INTO author_activity_windows (
					pubkey,
					window_days,
					day_of_week,
					hour_of_day,
					engagement_received,
					reply_received,
					reaction_received,
					repost_received,
					zap_received,
					derivation_version
				)
				SELECT
					$1,
					$3,
					d.day_of_week,
					d.hour_of_day,
					SUM(d.engagement_received)::bigint,
					SUM(d.reply_received)::bigint,
					SUM(d.reaction_received)::bigint,
					SUM(d.repost_received)::bigint,
					SUM(d.zap_received)::bigint,
					$4
				FROM author_hourly_activity d
				WHERE d.pubkey = $1
				  AND d.activity_date >= to_timestamp($2)::date
				GROUP BY d.day_of_week, d.hour_of_day
				HAVING SUM(d.engagement_received) > 0
				ON CONFLICT (pubkey, window_days, day_of_week, hour_of_day) DO UPDATE
				SET engagement_received = EXCLUDED.engagement_received,
				    reply_received = EXCLUDED.reply_received,
				    reaction_received = EXCLUDED.reaction_received,
				    repost_received = EXCLUDED.repost_received,
				    zap_received = EXCLUDED.zap_received,
				    derivation_version = EXCLUDED.derivation_version,
				    updated_at = now()
				WHERE author_activity_windows.engagement_received IS DISTINCT FROM EXCLUDED.engagement_received
				   OR author_activity_windows.reply_received IS DISTINCT FROM EXCLUDED.reply_received
				   OR author_activity_windows.reaction_received IS DISTINCT FROM EXCLUDED.reaction_received
				   OR author_activity_windows.repost_received IS DISTINCT FROM EXCLUDED.repost_received
				   OR author_activity_windows.zap_received IS DISTINCT FROM EXCLUDED.zap_received
				   OR author_activity_windows.derivation_version IS DISTINCT FROM EXCLUDED.derivation_version
			`, pubkey, cutoff, windowDays, version)
			if err != nil {
				return fmt.Errorf("upsert author activity windows from daily for %s window=%dd: %w", pubkey, windowDays, err)
			}
			return nil
		}
	}

	// Engagement sources read the denormalized target/source pubkeys
	// (migration 000082) instead of joining events per row, matching
	// projection_author_activity_daily. The old `INNER JOIN events target ON
	// target.pubkey = $1` plans heap-scanned every note a prolific author ever
	// wrote (a 310k-note bot cost ~2.4 GB of random events IO per attempt and
	// hit the statement timeout ~185 times/day in production, with zero
	// engagement in the window). Every branch is now one
	// (target_pubkey, created_at) index range scan proportional to the
	// author's own received engagement.
	_, err := tx.Exec(ctx, `
		WITH received_events AS (
			SELECT
				to_timestamp(rcc.source_created_at) AT TIME ZONE 'UTC' AS engaged_at,
				'reply'::text AS interaction_type
			FROM reply_count_contributions rcc
			WHERE rcc.target_pubkey = $1
			  AND rcc.source_pubkey <> $1
			  AND rcc.source_created_at >= $2
			  AND rcc.source_created_at <= $5
			UNION ALL
			SELECT
				to_timestamp(re.created_at) AT TIME ZONE 'UTC' AS engaged_at,
				'reaction'::text AS interaction_type
			FROM reaction_events re
			WHERE re.target_pubkey = $1
			  AND re.reactor_pubkey <> $1
			  AND re.created_at >= $2
			  AND re.created_at <= $5
			UNION ALL
			SELECT
				to_timestamp(re.created_at) AT TIME ZONE 'UTC' AS engaged_at,
				'repost'::text AS interaction_type
			FROM repost_events re
			WHERE re.target_pubkey = $1
			  AND re.reposter_pubkey <> $1
			  AND re.created_at >= $2
			  AND re.created_at <= $5
			UNION ALL
			SELECT
				to_timestamp(zr.created_at) AT TIME ZONE 'UTC' AS engaged_at,
				'zap'::text AS interaction_type
			FROM zap_receipts zr
			WHERE zr.receiver_pubkey = $1
			  AND zr.sender_pubkey IS NOT NULL
			  AND zr.sender_pubkey <> $1
			  AND zr.created_at >= $2
			  AND zr.created_at <= $5
		)
		INSERT INTO author_activity_windows (
			pubkey,
			window_days,
			day_of_week,
			hour_of_day,
			engagement_received,
			reply_received,
			reaction_received,
			repost_received,
			zap_received,
			derivation_version
		)
		SELECT
			$1,
			$3,
			EXTRACT(DOW FROM r.engaged_at)::smallint,
			EXTRACT(HOUR FROM r.engaged_at)::smallint,
			COUNT(*)::bigint,
			COUNT(*) FILTER (WHERE r.interaction_type = 'reply')::bigint,
			COUNT(*) FILTER (WHERE r.interaction_type = 'reaction')::bigint,
			COUNT(*) FILTER (WHERE r.interaction_type = 'repost')::bigint,
			COUNT(*) FILTER (WHERE r.interaction_type = 'zap')::bigint,
			$4
		FROM received_events r
		GROUP BY
			EXTRACT(DOW FROM r.engaged_at),
			EXTRACT(HOUR FROM r.engaged_at)
		ON CONFLICT (pubkey, window_days, day_of_week, hour_of_day) DO UPDATE
		SET engagement_received = EXCLUDED.engagement_received,
		    reply_received = EXCLUDED.reply_received,
		    reaction_received = EXCLUDED.reaction_received,
		    repost_received = EXCLUDED.repost_received,
		    zap_received = EXCLUDED.zap_received,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
		WHERE author_activity_windows.engagement_received IS DISTINCT FROM EXCLUDED.engagement_received
		   OR author_activity_windows.reply_received IS DISTINCT FROM EXCLUDED.reply_received
		   OR author_activity_windows.reaction_received IS DISTINCT FROM EXCLUDED.reaction_received
		   OR author_activity_windows.repost_received IS DISTINCT FROM EXCLUDED.repost_received
		   OR author_activity_windows.zap_received IS DISTINCT FROM EXCLUDED.zap_received
		   OR author_activity_windows.derivation_version IS DISTINCT FROM EXCLUDED.derivation_version
	`, pubkey, cutoff, windowDays, version, maxSaneUnixCreatedAt)
	if err != nil {
		return fmt.Errorf("upsert author activity windows for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}

func (h *Handlers) upsertAuthorPostingPatternsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	if h.incrementalWindowedRollups {
		var hasDaily bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM author_hourly_activity
				WHERE pubkey = $1
				  AND activity_date >= to_timestamp($2)::date
				  AND post_count > 0
			)
		`, pubkey, cutoff).Scan(&hasDaily); err != nil {
			return fmt.Errorf("check author_hourly_activity posts for %s: %w", pubkey, err)
		}
		if hasDaily {
			_, err := tx.Exec(ctx, `
				WITH agg AS (
					SELECT
						d.day_of_week,
						d.hour_of_day,
						SUM(d.post_count)::bigint AS post_count,
						SUM(d.note_count)::bigint AS note_count,
						SUM(d.reply_count)::bigint AS reply_count
					FROM author_hourly_activity d
					WHERE d.pubkey = $1
					  AND d.activity_date >= to_timestamp($2)::date
					GROUP BY d.day_of_week, d.hour_of_day
					HAVING SUM(d.post_count) > 0
				),
				upserted AS (
					INSERT INTO author_posting_patterns (
						pubkey, window_days, day_of_week, hour_of_day,
						post_count, note_count, reply_count, derivation_version
					)
					SELECT $1, $3, day_of_week, hour_of_day, post_count, note_count, reply_count, $4
					FROM agg
					ON CONFLICT (pubkey, window_days, day_of_week, hour_of_day) DO UPDATE
					SET post_count = EXCLUDED.post_count,
					    note_count = EXCLUDED.note_count,
					    reply_count = EXCLUDED.reply_count,
					    derivation_version = EXCLUDED.derivation_version,
					    updated_at = now()
					WHERE author_posting_patterns.post_count IS DISTINCT FROM EXCLUDED.post_count
					   OR author_posting_patterns.note_count IS DISTINCT FROM EXCLUDED.note_count
					   OR author_posting_patterns.reply_count IS DISTINCT FROM EXCLUDED.reply_count
					   OR author_posting_patterns.derivation_version IS DISTINCT FROM EXCLUDED.derivation_version
					RETURNING day_of_week
				)
				DELETE FROM author_posting_patterns t
				WHERE t.pubkey = $1
				  AND t.window_days = $3
				  AND NOT EXISTS (
				      SELECT 1 FROM agg a
				      WHERE a.day_of_week = t.day_of_week AND a.hour_of_day = t.hour_of_day
				  )
			`, pubkey, cutoff, windowDays, version)
			if err != nil {
				return fmt.Errorf("upsert author posting patterns from daily for %s window=%dd: %w", pubkey, windowDays, err)
			}
			return nil
		}
	}

	_, err := tx.Exec(ctx, `
		WITH authored AS (
			SELECT
				to_timestamp(e.created_at) AT TIME ZONE 'UTC' AS posted_at,
				EXISTS (
					SELECT 1
					FROM thread_edges te
					WHERE te.child_event_id = e.id
				) AS is_reply
			FROM events e
			WHERE e.pubkey = $1
			  AND e.kind = 1
			  AND e.created_at >= $2
			  AND e.created_at <= $5
		),
		agg AS (
			SELECT
				EXTRACT(DOW FROM a.posted_at)::smallint AS day_of_week,
				EXTRACT(HOUR FROM a.posted_at)::smallint AS hour_of_day,
				COUNT(*)::bigint AS post_count,
				COUNT(*) FILTER (WHERE NOT a.is_reply)::bigint AS note_count,
				COUNT(*) FILTER (WHERE a.is_reply)::bigint AS reply_count
			FROM authored a
			GROUP BY EXTRACT(DOW FROM a.posted_at), EXTRACT(HOUR FROM a.posted_at)
		),
		upserted AS (
			INSERT INTO author_posting_patterns (
				pubkey, window_days, day_of_week, hour_of_day,
				post_count, note_count, reply_count, derivation_version
			)
			SELECT $1, $3, day_of_week, hour_of_day, post_count, note_count, reply_count, $4
			FROM agg
			ON CONFLICT (pubkey, window_days, day_of_week, hour_of_day) DO UPDATE
			SET post_count = EXCLUDED.post_count,
			    note_count = EXCLUDED.note_count,
			    reply_count = EXCLUDED.reply_count,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
			WHERE author_posting_patterns.post_count IS DISTINCT FROM EXCLUDED.post_count
			   OR author_posting_patterns.note_count IS DISTINCT FROM EXCLUDED.note_count
			   OR author_posting_patterns.reply_count IS DISTINCT FROM EXCLUDED.reply_count
			   OR author_posting_patterns.derivation_version IS DISTINCT FROM EXCLUDED.derivation_version
			RETURNING day_of_week
		)
		DELETE FROM author_posting_patterns t
		WHERE t.pubkey = $1
		  AND t.window_days = $3
		  AND NOT EXISTS (
		      SELECT 1 FROM agg a
		      WHERE a.day_of_week = t.day_of_week AND a.hour_of_day = t.hour_of_day
		  )
	`, pubkey, cutoff, windowDays, version, maxSaneUnixCreatedAt)
	if err != nil {
		return fmt.Errorf("upsert author posting patterns for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}
