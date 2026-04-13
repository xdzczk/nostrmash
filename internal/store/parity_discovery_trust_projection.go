package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	discoveryProjectionModePreferTrusted = "prefer_trusted"
	discoveryProjectionModeTrustedOnly   = "trusted_only"
)

func (s *PostgresStore) GetTrustQualifiedTrendingNotes(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	mode string,
	policy TrustQualificationPolicy,
	maxStaleness time.Duration,
) ([]TrustQualifiedTrendingNote, bool, error) {
	if s == nil || s.pool == nil {
		return nil, false, fmt.Errorf("store is not initialized")
	}
	ready, err := s.trustedDiscoveryProjectionReady(ctx, "trusted_note_discovery_candidates", maxStaleness)
	if err != nil || !ready {
		return nil, ready, err
	}
	scoreColumn, windowDuration, err := resolveTrendingWindow(window)
	if err != nil {
		return nil, false, err
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	minCreatedAt := time.Now().UTC().Add(-windowDuration).Unix()
	trustedExpr := "(t.min_hops IS NOT NULL AND ($2::integer = 0 OR t.min_hops <= $2::integer) AND ($3::double precision <= 0 OR COALESCE(t.trust_score, 0) >= $3::double precision))"
	whereTrusted := ""
	orderBy := fmt.Sprintf("n.%s DESC, n.created_at DESC, n.event_id ASC", scoreColumn)
	if mode == discoveryProjectionModeTrustedOnly {
		whereTrusted = "AND " + trustedExpr
	}
	if mode == discoveryProjectionModePreferTrusted {
		orderBy = fmt.Sprintf("%s DESC, n.%s DESC, n.created_at DESC, n.event_id ASC", trustedExpr, scoreColumn)
	}
	query := fmt.Sprintf(`
		SELECT
			n.event_id,
			n.author_pubkey,
			n.created_at,
			e.content,
			COALESCE(n.primary_language, 'und') AS language,
			n.reply_count,
			n.repost_count,
			n.reaction_count,
			n.zap_count,
			n.zap_msats,
			n.%s AS score,
			%s AS trusted
		FROM note_discovery_stats n
		INNER JOIN trusted_note_discovery_candidates t ON t.event_id = n.event_id
		INNER JOIN events e ON e.id = n.event_id
		WHERE n.created_at >= $1
		  %s
		ORDER BY %s
		LIMIT $4 OFFSET $5
	`, scoreColumn, trustedExpr, whereTrusted, orderBy)
	rows, err := s.pool.Query(ctx, query, minCreatedAt, policy.MaxHops, policy.MinimumScore, limit, offset)
	if err != nil {
		return nil, false, fmt.Errorf("get trust-qualified trending notes: %w", err)
	}
	defer rows.Close()
	out := make([]TrustQualifiedTrendingNote, 0, limit)
	for rows.Next() {
		var row TrustQualifiedTrendingNote
		if err := rows.Scan(
			&row.Note.EventID,
			&row.Note.AuthorPubkey,
			&row.Note.CreatedAt,
			&row.Note.Content,
			&row.Note.Language,
			&row.Note.ReplyCount,
			&row.Note.RepostCount,
			&row.Note.ReactionCount,
			&row.Note.ZapCount,
			&row.Note.ZapMSats,
			&row.Note.Score,
			&row.Trusted,
		); err != nil {
			return nil, false, fmt.Errorf("scan trust-qualified trending note row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read trust-qualified trending note rows: %w", err)
	}
	return out, true, nil
}

func (s *PostgresStore) GetTrustQualifiedTrendingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	rising bool,
	mode string,
	policy TrustQualificationPolicy,
	maxStaleness time.Duration,
) ([]TrustQualifiedTrendingProfile, bool, error) {
	if s == nil || s.pool == nil {
		return nil, false, fmt.Errorf("store is not initialized")
	}
	ready, err := s.trustedDiscoveryProjectionReady(ctx, "trusted_profile_discovery_candidates", maxStaleness)
	if err != nil || !ready {
		return nil, ready, err
	}
	scoreColumn, windowDuration, err := resolveTrendingWindow(window)
	if err != nil {
		return nil, false, err
	}
	if rising {
		if scoreColumn == "score_24h" {
			scoreColumn = "rising_score_24h"
		} else {
			scoreColumn = "rising_score_7d"
		}
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	minCreatedAt := time.Now().UTC().Add(-windowDuration).Unix()
	trustedExpr := "(t.min_hops IS NOT NULL AND ($2::integer = 0 OR t.min_hops <= $2::integer) AND ($3::double precision <= 0 OR COALESCE(t.trust_score, 0) >= $3::double precision))"
	whereTrusted := ""
	orderBy := fmt.Sprintf("p.%s DESC, p.recent_engagement_received DESC, p.recent_activity_at DESC, p.pubkey ASC", scoreColumn)
	if mode == discoveryProjectionModeTrustedOnly {
		whereTrusted = "AND " + trustedExpr
	}
	if mode == discoveryProjectionModePreferTrusted {
		orderBy = fmt.Sprintf("%s DESC, p.%s DESC, p.recent_engagement_received DESC, p.recent_activity_at DESC, p.pubkey ASC", trustedExpr, scoreColumn)
	}
	query := fmt.Sprintf(`
		SELECT
			p.pubkey,
			p.%s AS score,
			p.recent_post_count,
			p.recent_reply_count,
			p.recent_engagement_received,
			COALESCE((
				SELECT COUNT(*)
				FROM follower_edges fe
				WHERE fe.followed_pubkey = p.pubkey
				  AND fe.contact_list_created_at >= $1
			), 0)::bigint AS recent_new_followers,
			p.recent_zap_volume_msats,
			p.recent_active_days,
			p.recent_activity_at,
			%s AS trusted
		FROM profile_discovery_stats p
		INNER JOIN trusted_profile_discovery_candidates t ON t.pubkey = p.pubkey
		WHERE p.recent_activity_at IS NOT NULL
		  AND p.recent_activity_at >= $1
		  AND p.%s > 0
		  %s
		ORDER BY %s
		LIMIT $4 OFFSET $5
	`, scoreColumn, trustedExpr, scoreColumn, whereTrusted, orderBy)
	rows, err := s.pool.Query(ctx, query, minCreatedAt, policy.MaxHops, policy.MinimumScore, limit, offset)
	if err != nil {
		return nil, false, fmt.Errorf("get trust-qualified trending profiles: %w", err)
	}
	defer rows.Close()
	out := make([]TrustQualifiedTrendingProfile, 0, limit)
	for rows.Next() {
		var row TrustQualifiedTrendingProfile
		if err := rows.Scan(
			&row.Profile.Pubkey,
			&row.Profile.Score,
			&row.Profile.RecentPostCount,
			&row.Profile.RecentReplyCount,
			&row.Profile.RecentEngagementReceived,
			&row.Profile.RecentNewFollowers,
			&row.Profile.RecentZapVolumeMSats,
			&row.Profile.RecentActiveDays,
			&row.Profile.RecentActivityAt,
			&row.Trusted,
		); err != nil {
			return nil, false, fmt.Errorf("scan trust-qualified trending profile row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read trust-qualified trending profile rows: %w", err)
	}
	return out, true, nil
}

func (s *PostgresStore) trustedDiscoveryProjectionReady(
	ctx context.Context,
	projectionName string,
	maxStaleness time.Duration,
) (bool, error) {
	var (
		refreshedAt         time.Time
		projectedSnapshotAt *time.Time
		currentSnapshotAt   *time.Time
	)
	if err := s.pool.QueryRow(ctx, `
		SELECT
			state.refreshed_at,
			state.trust_snapshot_refreshed_at,
			(SELECT MAX(refreshed_at) FROM trust_graph_snapshot) AS current_snapshot_refreshed_at
		FROM trusted_discovery_projection_state state
		WHERE state.projection_name = $1
	`, projectionName).Scan(&refreshedAt, &projectedSnapshotAt, &currentSnapshotAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("read trusted discovery projection state: %w", err)
	}
	if currentSnapshotAt != nil {
		if projectedSnapshotAt == nil || projectedSnapshotAt.Before(*currentSnapshotAt) {
			return false, nil
		}
	}
	if maxStaleness > 0 {
		cutoff := time.Now().UTC().Add(-maxStaleness)
		if refreshedAt.Before(cutoff) {
			return false, nil
		}
	}
	return true, nil
}
