package read

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

func (s *Read) ListFollowedPubkeys(ctx context.Context, viewer string, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	viewer = strings.TrimSpace(viewer)
	if viewer == "" {
		return nil, fmt.Errorf("viewer pubkey is required")
	}
	if limit <= 0 {
		limit = 2000
	}
	rows, err := s.pool.Query(ctx, `
		SELECT fe.followed_pubkey
		FROM follower_edges fe
		LEFT JOIN trust_pubkeys_latest t ON t.pubkey = fe.followed_pubkey
		WHERE fe.follower_pubkey = $1
		ORDER BY t.rank ASC NULLS LAST, fe.followed_pubkey ASC
		LIMIT $2
	`, viewer, limit)
	if err != nil {
		return nil, fmt.Errorf("list followed pubkeys: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var followed string
		if err := rows.Scan(&followed); err != nil {
			return nil, fmt.Errorf("scan followed pubkey: %w", err)
		}
		followed = strings.TrimSpace(followed)
		if followed == "" {
			continue
		}
		out = append(out, followed)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read followed pubkeys: %w", err)
	}
	return out, nil
}

var errEmptyCohort = errors.New("empty cohort")

func (s *Read) beginCohort(follows []string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	if len(follows) == 0 {
		return errEmptyCohort
	}
	return nil
}

func emptyCohort(err error) bool {
	return errors.Is(err, errEmptyCohort)
}

func clampCohortLimit(limit int) int {
	if limit <= 0 {
		return 300
	}
	if limit > discoveryReadLimit {
		return discoveryReadLimit
	}
	return limit
}
