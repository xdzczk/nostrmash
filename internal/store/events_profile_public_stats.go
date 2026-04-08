package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// GetProfilePublicStatsByPubkey fetches projected public profile counters.
func (s *PostgresStore) GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (ProfilePublicStatsProjection, error) {
	out := ProfilePublicStatsProjection{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return out, fmt.Errorf("pubkey is required")
	}

	var recentActivityAt *int64
	err := s.pool.QueryRow(ctx, `
		SELECT
			pubkey,
			follower_count,
			following_count,
			note_count,
			reply_count,
			recent_activity_at
		FROM profile_public_stats
		WHERE pubkey = $1
	`, pubkey).Scan(
		&out.Pubkey,
		&out.FollowerCount,
		&out.FollowingCount,
		&out.NoteCount,
		&out.ReplyCount,
		&recentActivityAt,
	)
	if err != nil {
		// Missing stats are allowed while projections catch up.
		if errors.Is(err, pgx.ErrNoRows) {
			out.Pubkey = pubkey
			return out, nil
		}
		return out, fmt.Errorf("get profile public stats by pubkey: %w", err)
	}
	out.RecentActivityAt = recentActivityAt
	return out, nil
}
