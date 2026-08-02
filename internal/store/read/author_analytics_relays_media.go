package read

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/model"
)

func (s *Read) GetAuthorRelayFootprint(
	ctx context.Context,
	pubkey string,
	topRelayLimit int,
) (AuthorRelayFootprintProjection, error) {
	out := AuthorRelayFootprintProjection{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return out, fmt.Errorf("pubkey is required")
	}
	if topRelayLimit <= 0 {
		topRelayLimit = 8
	}
	if topRelayLimit > 20 {
		topRelayLimit = 20
	}
	out.Pubkey = pubkey
	// Reads pubkey directly from event_relays (denormalized in
	// migration 000045) so the per-author footprint no longer has to
	// JOIN events just to filter by author.
	if err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(COUNT(DISTINCT er.relay_url), 0)::bigint AS relay_count,
			COALESCE(COUNT(DISTINCT er.event_id), 0)::bigint AS seen_on_event_count
		FROM event_relays er
		WHERE er.pubkey = $1
		  AND er.relay_url <> $2
	`, pubkey, model.FallbackRelayURL).Scan(&out.RelayCount, &out.SeenOnEventCount); err != nil {
		return out, fmt.Errorf("get author relay footprint counts: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			er.relay_url,
			COUNT(DISTINCT er.event_id)::bigint AS event_count
		FROM event_relays er
		WHERE er.pubkey = $1
		  AND er.relay_url <> $2
		GROUP BY er.relay_url
		ORDER BY event_count DESC, er.relay_url ASC
		LIMIT $3
	`, pubkey, model.FallbackRelayURL, topRelayLimit)
	if err != nil {
		return out, fmt.Errorf("get author relay footprint top relays: %w", err)
	}
	defer rows.Close()

	out.TopRelays = make([]RelayUsageSummary, 0, topRelayLimit)
	for rows.Next() {
		var row RelayUsageSummary
		if err := rows.Scan(&row.RelayURL, &row.EventCount); err != nil {
			return out, fmt.Errorf("scan author relay footprint row: %w", err)
		}
		row.UniqueAuthors = 1
		out.TopRelays = append(out.TopRelays, row)
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("read author relay footprint rows: %w", err)
	}
	return out, nil
}

func (s *Read) GetAuthorMediaMixStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorMediaMixStatsProjection, error) {
	out := AuthorMediaMixStatsProjection{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return out, fmt.Errorf("pubkey is required")
	}

	err := s.pool.QueryRow(ctx, `
		SELECT
			pubkey,
			window_days,
			total_posts,
			with_image_count,
			with_video_count,
			with_link_count,
			with_article_count,
			text_only_count,
			total_attachment_count
		FROM author_media_mix_stats
		WHERE pubkey = $1
		  AND window_days = $2
	`, pubkey, windowDays).Scan(
		&out.Pubkey,
		&out.WindowDays,
		&out.TotalPosts,
		&out.WithImageCount,
		&out.WithVideoCount,
		&out.WithLinkCount,
		&out.WithArticleCount,
		&out.TextOnlyCount,
		&out.TotalAttachmentCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			out.Pubkey = pubkey
			out.WindowDays = windowDays
			return out, nil
		}
		return out, fmt.Errorf("get author media mix stats: %w", err)
	}
	return out, nil
}
