package read

import (
	"context"
	"fmt"
)

// FindActiveAuthorsWithoutMetadata returns pubkeys that have authored public
// notes but have no entry in profiles_latest. These are candidates for
// relay-based metadata discovery.
//
// Implementation note: the query intentionally drives off
// profile_public_stats rather than scanning the (much larger) events table.
//
//   - profile_public_stats has at most one row per pubkey and is updated by
//     the per-event derivation pipeline whenever any public-author event
//     (kind 1, 6, 7, 3, ...) is processed, so every active note author ends
//     up represented there.
//   - The table has an index on (recent_activity_at DESC NULLS LAST, pubkey)
//     which lets PostgreSQL satisfy the ORDER BY without a sort and stop
//     scanning as soon as the LIMIT is satisfied.
//   - The anti-join into profiles_latest is a primary-key lookup.
//
// The previous implementation issued
//
//	SELECT DISTINCT e.pubkey FROM events e WHERE e.kind = 1 AND NOT EXISTS (...)
//
// which forced a full scan + dedupe over the entire events table together
// with two correlated NOT EXISTS subqueries against events itself. On a
// multi-million-row table that query took >15 minutes per execution and
// pinned database connections, starving the rest of the system.
//
// Authors that exist in events but have not yet been projected into
// profile_public_stats (e.g., during initial backlog processing) are
// temporarily invisible to discovery; they will be picked up automatically
// on the next cycle once the projection catches up. This is an acceptable
// trade-off because the discovery loop runs on a steady cadence and the
// scan-based query was so slow it actively prevented projections from
// catching up at all.
func (s *Read) FindActiveAuthorsWithoutMetadata(ctx context.Context, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	rows, err := s.pool.Query(ctx, `
		SELECT pps.pubkey
		FROM profile_public_stats pps
		WHERE pps.note_count > 0
		  AND NOT EXISTS (
			SELECT 1 FROM profiles_latest pl
			WHERE pl.pubkey = pps.pubkey
		  )
		ORDER BY pps.recent_activity_at DESC NULLS LAST, pps.pubkey ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("find active authors without metadata: %w", err)
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, fmt.Errorf("scan author pubkey: %w", err)
		}
		out = append(out, pubkey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author pubkey rows: %w", err)
	}
	return out, nil
}

// CountActiveAuthorsWithoutMetadata returns the number of note authors that
// have no kind-0 metadata projected. See FindActiveAuthorsWithoutMetadata for
// the rationale behind driving off profile_public_stats.
func (s *Read) CountActiveAuthorsWithoutMetadata(ctx context.Context) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	var count int64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM profile_public_stats pps
		WHERE pps.note_count > 0
		  AND NOT EXISTS (
			SELECT 1 FROM profiles_latest pl
			WHERE pl.pubkey = pps.pubkey
		  )
	`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active authors without metadata: %w", err)
	}
	return count, nil
}
