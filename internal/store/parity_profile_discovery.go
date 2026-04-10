package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (s *PostgresStore) GetTrendingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
	return s.getProfileDiscoveryRows(ctx, window, limit, offset, false)
}

func (s *PostgresStore) GetRisingProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
	return s.getProfileDiscoveryRows(ctx, window, limit, offset, true)
}

// GetRelatedProfiles returns bounded related-profile candidates for one focal pubkey.
func (s *PostgresStore) GetRelatedProfiles(ctx context.Context, pubkey string, limit int) ([]RelatedProfile, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT
			EXISTS(SELECT 1 FROM profiles_latest WHERE pubkey = $1)
			OR EXISTS(SELECT 1 FROM events WHERE pubkey = $1)
	`, pubkey).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check focal profile exists: %w", err)
	}
	if !exists {
		return nil, ErrNotFound
	}
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour).Unix()
	rows, err := s.pool.Query(ctx, `
		WITH focal_notes AS (
			SELECT e.id
			FROM events e
			WHERE e.pubkey = $1
			  AND e.kind = 1
			  AND e.created_at >= $3
			ORDER BY e.created_at DESC, e.id DESC
			LIMIT 200
		),
		focal_hashtags AS (
			SELECT eh.hashtag
			FROM event_hashtags eh
			JOIN focal_notes fn ON fn.id = eh.event_id
			GROUP BY eh.hashtag
			ORDER BY COUNT(*) DESC, eh.hashtag ASC
			LIMIT 30
		),
		topic_candidates AS (
			SELECT
				e.pubkey,
				COUNT(*)::bigint AS topic_overlap,
				LEAST(80, COUNT(*) * 6)::bigint AS score
			FROM focal_hashtags fh
			JOIN event_hashtags eh ON eh.hashtag = fh.hashtag
			JOIN events e ON e.id = eh.event_id
			WHERE e.kind = 1
			  AND e.pubkey <> $1
			  AND e.created_at >= $3
			GROUP BY e.pubkey
			ORDER BY topic_overlap DESC, e.pubkey ASC
			LIMIT 150
		),
		reply_inbound AS (
			SELECT
				child.pubkey,
				COUNT(*)::bigint AS count_value
			FROM thread_edges te
			JOIN events parent ON parent.id = te.parent_event_id
			JOIN events child ON child.id = te.child_event_id
			WHERE parent.pubkey = $1
			  AND child.pubkey <> $1
			  AND child.created_at >= $3
			GROUP BY child.pubkey
		),
		reply_outbound AS (
			SELECT
				parent.pubkey,
				COUNT(*)::bigint AS count_value
			FROM thread_edges te
			JOIN events child ON child.id = te.child_event_id
			JOIN events parent ON parent.id = te.parent_event_id
			WHERE child.pubkey = $1
			  AND parent.pubkey <> $1
			  AND child.created_at >= $3
			GROUP BY parent.pubkey
		),
		reply_candidates AS (
			SELECT
				pubkey,
				SUM(count_value)::bigint AS reply_adjacency,
				LEAST(90, SUM(count_value) * 10)::bigint AS score
			FROM (
				SELECT * FROM reply_inbound
				UNION ALL
				SELECT * FROM reply_outbound
			) merged
			GROUP BY pubkey
			ORDER BY reply_adjacency DESC, pubkey ASC
			LIMIT 150
		),
		reaction_inbound AS (
			SELECT
				re.reactor_pubkey AS pubkey,
				COUNT(*)::bigint AS count_value
			FROM reaction_events re
			JOIN events target ON target.id = re.target_event_id
			WHERE target.pubkey = $1
			  AND re.reactor_pubkey <> $1
			  AND re.created_at >= $3
			GROUP BY re.reactor_pubkey
		),
		reaction_outbound AS (
			SELECT
				target.pubkey,
				COUNT(*)::bigint AS count_value
			FROM reaction_events re
			JOIN events target ON target.id = re.target_event_id
			WHERE re.reactor_pubkey = $1
			  AND target.pubkey <> $1
			  AND re.created_at >= $3
			GROUP BY target.pubkey
		),
		interaction_candidates AS (
			SELECT
				pubkey,
				SUM(count_value)::bigint AS interaction_adjacency,
				LEAST(70, SUM(count_value) * 8)::bigint AS score
			FROM (
				SELECT * FROM reaction_inbound
				UNION ALL
				SELECT * FROM reaction_outbound
			) merged
			GROUP BY pubkey
			ORDER BY interaction_adjacency DESC, pubkey ASC
			LIMIT 150
		),
		repost_inbound AS (
			SELECT
				re.reposter_pubkey AS pubkey,
				COUNT(*)::bigint AS count_value
			FROM repost_events re
			JOIN events target ON target.id = re.target_event_id
			WHERE target.pubkey = $1
			  AND re.reposter_pubkey <> $1
			  AND re.created_at >= $3
			GROUP BY re.reposter_pubkey
		),
		repost_outbound AS (
			SELECT
				target.pubkey,
				COUNT(*)::bigint AS count_value
			FROM repost_events re
			JOIN events target ON target.id = re.target_event_id
			WHERE re.reposter_pubkey = $1
			  AND target.pubkey <> $1
			  AND re.created_at >= $3
			GROUP BY target.pubkey
		),
		quote_repost_candidates AS (
			SELECT
				pubkey,
				SUM(count_value)::bigint AS quote_repost_adjacency,
				LEAST(100, SUM(count_value) * 12)::bigint AS score
			FROM (
				SELECT * FROM repost_inbound
				UNION ALL
				SELECT * FROM repost_outbound
			) merged
			GROUP BY pubkey
			ORDER BY quote_repost_adjacency DESC, pubkey ASC
			LIMIT 150
		),
		unioned AS (
			SELECT
				tc.pubkey,
				tc.topic_overlap,
				0::bigint AS reply_adjacency,
				0::bigint AS interaction_adjacency,
				0::bigint AS quote_repost_adjacency,
				'topic_overlap'::text AS reason,
				tc.score
			FROM topic_candidates tc
			UNION ALL
			SELECT
				rc.pubkey,
				0::bigint,
				rc.reply_adjacency,
				0::bigint,
				0::bigint,
				'reply_adjacency'::text,
				rc.score
			FROM reply_candidates rc
			UNION ALL
			SELECT
				ic.pubkey,
				0::bigint,
				0::bigint,
				ic.interaction_adjacency,
				0::bigint,
				'interaction_adjacency'::text,
				ic.score
			FROM interaction_candidates ic
			UNION ALL
			SELECT
				qc.pubkey,
				0::bigint,
				0::bigint,
				0::bigint,
				qc.quote_repost_adjacency,
				'quote_repost_adjacency'::text,
				qc.score
			FROM quote_repost_candidates qc
		),
		ranked AS (
			SELECT
				u.pubkey,
				MAX(u.topic_overlap)::bigint AS topic_overlap,
				MAX(u.reply_adjacency)::bigint AS reply_adjacency,
				MAX(u.interaction_adjacency)::bigint AS interaction_adjacency,
				MAX(u.quote_repost_adjacency)::bigint AS quote_repost_adjacency,
				ARRAY_AGG(DISTINCT u.reason ORDER BY u.reason) AS reasons,
				(SUM(u.score) + COUNT(*))::bigint AS rank_score
			FROM unioned u
			WHERE EXISTS (
				SELECT 1
				FROM profiles_latest pl
				WHERE pl.pubkey = u.pubkey
			)
			OR EXISTS (
				SELECT 1
				FROM events metadata
				WHERE metadata.pubkey = u.pubkey
				  AND metadata.kind = 0
			)
			GROUP BY u.pubkey
			ORDER BY rank_score DESC, u.pubkey ASC
			LIMIT $2
		)
		SELECT
			r.pubkey,
			r.topic_overlap,
			r.reply_adjacency,
			r.interaction_adjacency,
			r.quote_repost_adjacency,
			r.reasons,
			r.rank_score
		FROM ranked r
		ORDER BY
			r.rank_score DESC,
			r.quote_repost_adjacency DESC,
			r.reply_adjacency DESC,
			r.topic_overlap DESC,
			r.interaction_adjacency DESC,
			r.pubkey ASC
	`, pubkey, limit, cutoff)
	if err != nil {
		return nil, fmt.Errorf("get related profiles: %w", err)
	}
	defer rows.Close()
	out := make([]RelatedProfile, 0, limit)
	for rows.Next() {
		var row RelatedProfile
		if err := rows.Scan(
			&row.Pubkey,
			&row.TopicOverlap,
			&row.ReplyAdjacency,
			&row.InteractionAdjacency,
			&row.QuoteRepostAdjacency,
			&row.Reasons,
			&row.Score,
		); err != nil {
			return nil, fmt.Errorf("scan related profile row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read related profile rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) getProfileDiscoveryRows(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	rising bool,
) ([]TrendingProfile, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	scoreColumn, windowDuration, err := resolveTrendingWindow(window)
	if err != nil {
		return nil, err
	}
	if rising {
		switch scoreColumn {
		case "score_24h":
			scoreColumn = "rising_score_24h"
		case "score_7d":
			scoreColumn = "rising_score_7d"
		}
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	minCreatedAt := time.Now().UTC().Add(-windowDuration).Unix()
	query := fmt.Sprintf(`
		SELECT
			pubkey,
			%s AS score,
			recent_post_count,
			recent_reply_count,
			recent_engagement_received,
			recent_zap_volume_msats,
			recent_active_days,
			recent_activity_at
		FROM profile_discovery_stats
		WHERE recent_activity_at IS NOT NULL
		  AND recent_activity_at >= $1
		  AND %s > 0
		  AND (
			EXISTS (
				SELECT 1
				FROM profiles_latest pl
				WHERE pl.pubkey = profile_discovery_stats.pubkey
			)
			OR EXISTS (
				SELECT 1
				FROM events metadata
				WHERE metadata.pubkey = profile_discovery_stats.pubkey
				  AND metadata.kind = 0
			)
		  )
		  AND EXISTS (
			SELECT 1
			FROM events e
			WHERE e.pubkey = profile_discovery_stats.pubkey
			  AND e.kind = 1
			  AND e.created_at >= $1
			  AND NOT EXISTS (
				SELECT 1
				FROM event_references er
				WHERE er.source_event_id = e.id
				  AND er.relation = 'reply'
			  )
		  )
		ORDER BY score DESC, recent_engagement_received DESC, recent_activity_at DESC, pubkey ASC
		LIMIT $2 OFFSET $3
	`, scoreColumn, scoreColumn)
	rows, err := s.pool.Query(ctx, query, minCreatedAt, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get profile discovery rows: %w", err)
	}
	defer rows.Close()

	out := make([]TrendingProfile, 0, limit)
	for rows.Next() {
		var row TrendingProfile
		if err := rows.Scan(
			&row.Pubkey,
			&row.Score,
			&row.RecentPostCount,
			&row.RecentReplyCount,
			&row.RecentEngagementReceived,
			&row.RecentZapVolumeMSats,
			&row.RecentActiveDays,
			&row.RecentActivityAt,
		); err != nil {
			return nil, fmt.Errorf("scan profile discovery row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read profile discovery rows: %w", err)
	}
	return out, nil
}
