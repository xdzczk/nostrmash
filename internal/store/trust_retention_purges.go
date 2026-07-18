package store

import (
	"context"
	"fmt"
	"time"
)

// PurgeStaleTrustedDiscoveryCandidates deletes trusted discovery candidate
// rows (notes and profiles) whose projection is stale: graph-qualified rows
// (min_hops IS NOT NULL) older than trustedBefore, unqualified rows older
// than untrustedBefore. Candidates are bulk-refreshed with every trust
// snapshot, so steady-state rows keep a fresh projected_at and survive; only
// rows a refresh no longer touches decay out. Both tables are rebuildable
// from the trust snapshot plus discovery stats.
func (s *PostgresStore) PurgeStaleTrustedDiscoveryCandidates(
	ctx context.Context,
	trustedBefore time.Time,
	untrustedBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if trustedBefore.IsZero() || untrustedBefore.IsZero() {
		return 0, fmt.Errorf("trustedBefore and untrustedBefore are required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	var total int64
	noteTag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT event_id
			FROM trusted_note_discovery_candidates
			WHERE (min_hops IS NOT NULL AND projected_at < $1)
			   OR (min_hops IS NULL AND projected_at < $2)
			LIMIT $3
		)
		DELETE FROM trusted_note_discovery_candidates t
		USING candidates c
		WHERE t.event_id = c.event_id
	`, trustedBefore.UTC(), untrustedBefore.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge stale trusted note discovery candidates: %w", err)
	}
	total += noteTag.RowsAffected()

	profileTag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT pubkey
			FROM trusted_profile_discovery_candidates
			WHERE (min_hops IS NOT NULL AND projected_at < $1)
			   OR (min_hops IS NULL AND projected_at < $2)
			LIMIT $3
		)
		DELETE FROM trusted_profile_discovery_candidates t
		USING candidates c
		WHERE t.pubkey = c.pubkey
	`, trustedBefore.UTC(), untrustedBefore.UTC(), limit)
	if err != nil {
		return total, fmt.Errorf("purge stale trusted profile discovery candidates: %w", err)
	}
	return total + profileTag.RowsAffected(), nil
}

// PurgeIdleAccountStates deletes low-value account_states rows: accounts still
// in the 'unknown' or 'observed' lifecycle states, with no manual override,
// whose last observation is older than the trust-aware horizon (trustedBefore
// for pubkeys in trust_graph_snapshot, untrustedBefore otherwise). Deleted
// rows are recreated with default state on the next observation; only the
// observation count signal is lost, which is the accepted cost of bounding a
// table that otherwise grows one row per pubkey ever seen on the firehose.
// Accounts at 'candidate' or above are never touched.
func (s *PostgresStore) PurgeIdleAccountStates(
	ctx context.Context,
	trustedBefore time.Time,
	untrustedBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if trustedBefore.IsZero() || untrustedBefore.IsZero() {
		return 0, fmt.Errorf("trustedBefore and untrustedBefore are required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	tag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT a.pubkey
			FROM account_states a
			WHERE a.state IN ('unknown', 'observed')
			  AND a.manual_override IS NULL
			  AND a.last_observed_at < CASE
				WHEN EXISTS (
					SELECT 1 FROM trust_graph_snapshot s WHERE s.pubkey = a.pubkey
				) THEN $1::timestamptz
				ELSE $2::timestamptz
			  END
			LIMIT $3
		)
		DELETE FROM account_states a
		USING candidates c
		WHERE a.pubkey = c.pubkey
	`, trustedBefore.UTC(), untrustedBefore.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge idle account states: %w", err)
	}
	return tag.RowsAffected(), nil
}
