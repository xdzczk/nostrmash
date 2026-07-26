package derivation

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) upsertAuthorTopicWindowTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM author_topic_stats
		WHERE pubkey = $1
		  AND window_days = $2
	`, pubkey, windowDays); err != nil {
		return fmt.Errorf("delete prior author topic stats for %s window=%dd: %w", pubkey, windowDays, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO author_topic_stats (
			pubkey,
			window_days,
			hashtag,
			usage_count,
			active_days,
			derivation_version
		)
		SELECT
			$1,
			$2,
			eh.hashtag,
			COUNT(*)::bigint AS usage_count,
			COUNT(DISTINCT to_timestamp(eh.created_at)::date)::int AS active_days,
			$3
		FROM event_hashtags eh
		WHERE eh.author_pubkey = $1
		  AND eh.created_at >= $4
		  AND eh.created_at <= $5
		GROUP BY eh.hashtag
		ON CONFLICT (pubkey, window_days, hashtag) DO UPDATE
		SET usage_count = EXCLUDED.usage_count,
		    active_days = EXCLUDED.active_days,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, windowDays, version, cutoff, maxSaneUnixCreatedAt); err != nil {
		return fmt.Errorf("upsert author topic stats for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}

func (h *Handlers) upsertAuthorMediaMixWindowTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	_, err := tx.Exec(ctx, `
		WITH media_events AS (
			SELECT
				nds.has_image,
				nds.has_video,
				nds.has_link,
				nds.has_article,
				nds.attachment_count
			FROM note_discovery_stats nds
			WHERE nds.author_pubkey = $1
			  AND nds.created_at >= $2
		),
		classified AS (
			SELECT
				has_image,
				has_video,
				has_link,
				has_article,
				attachment_count
			FROM media_events
		)
		INSERT INTO author_media_mix_stats (
			pubkey,
			window_days,
			total_posts,
			with_image_count,
			with_video_count,
			with_link_count,
			with_article_count,
			text_only_count,
			total_attachment_count,
			derivation_version
		)
		SELECT
			$1,
			$3,
			COUNT(*)::bigint,
			COUNT(*) FILTER (WHERE has_image)::bigint,
			COUNT(*) FILTER (WHERE has_video)::bigint,
			COUNT(*) FILTER (WHERE has_link)::bigint,
			COUNT(*) FILTER (WHERE has_article)::bigint,
			COUNT(*) FILTER (WHERE NOT has_image AND NOT has_video AND NOT has_link AND NOT has_article)::bigint,
			COALESCE(SUM(attachment_count), 0)::bigint,
			$4
		FROM classified
		ON CONFLICT (pubkey, window_days) DO UPDATE
		SET total_posts = EXCLUDED.total_posts,
		    with_image_count = EXCLUDED.with_image_count,
		    with_video_count = EXCLUDED.with_video_count,
		    with_link_count = EXCLUDED.with_link_count,
		    with_article_count = EXCLUDED.with_article_count,
		    text_only_count = EXCLUDED.text_only_count,
		    total_attachment_count = EXCLUDED.total_attachment_count,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, cutoff, windowDays, version)
	if err != nil {
		return fmt.Errorf("upsert author media mix stats for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}
