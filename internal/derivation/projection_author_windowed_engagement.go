package derivation

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) upsertAuthorEngagementWindowTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO author_engagement_stats (
			pubkey,
			window_days,
			post_count,
			note_count,
			reply_count,
			active_days,
			engagement_received,
			engagement_given,
			cadence_posts_per_day,
			cadence_posts_per_active_day,
			recent_activity_at,
			derivation_version
		)
		SELECT
			$1 AS pubkey,
			$2 AS window_days,
			COALESCE(SUM(a.post_count), 0)::bigint AS post_count,
			COALESCE(SUM(a.note_count), 0)::bigint AS note_count,
			COALESCE(SUM(a.reply_count), 0)::bigint AS reply_count,
			COALESCE(COUNT(*) FILTER (WHERE a.post_count > 0), 0)::int AS active_days,
			COALESCE(SUM(a.engagement_received), 0)::bigint AS engagement_received,
			COALESCE(SUM(a.engagement_given), 0)::bigint AS engagement_given,
			COALESCE(SUM(a.post_count), 0)::double precision / $5::double precision AS cadence_posts_per_day,
			CASE
				WHEN COALESCE(COUNT(*) FILTER (WHERE a.post_count > 0), 0) = 0 THEN 0
				ELSE COALESCE(SUM(a.post_count), 0)::double precision
					/ (COUNT(*) FILTER (WHERE a.post_count > 0))::double precision
			END AS cadence_posts_per_active_day,
			(
				SELECT MAX(e.created_at)
				FROM events e
				WHERE e.pubkey = $1
				  AND e.kind = 1
				  AND e.created_at >= $3
			) AS recent_activity_at,
			$4 AS derivation_version
		FROM author_activity_daily a
		WHERE a.pubkey = $1
		  AND a.activity_date >= to_timestamp($3)::date
		ON CONFLICT (pubkey, window_days) DO UPDATE
		SET post_count = EXCLUDED.post_count,
		    note_count = EXCLUDED.note_count,
		    reply_count = EXCLUDED.reply_count,
		    active_days = EXCLUDED.active_days,
		    engagement_received = EXCLUDED.engagement_received,
		    engagement_given = EXCLUDED.engagement_given,
		    cadence_posts_per_day = EXCLUDED.cadence_posts_per_day,
		    cadence_posts_per_active_day = EXCLUDED.cadence_posts_per_active_day,
		    recent_activity_at = EXCLUDED.recent_activity_at,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, windowDays, cutoff, version, windowDays)
	if err != nil {
		return fmt.Errorf("upsert author engagement stats for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}

func computeWindowCutoff(windowDays int) int64 {
	if windowDays <= 1 {
		return 0
	}
	seconds := int64(windowDays * 24 * 60 * 60)
	return time.Now().UTC().Unix() - seconds
}
