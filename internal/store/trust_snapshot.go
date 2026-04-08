package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type TrustQualificationPolicy struct {
	MaxHops      int
	MinimumScore float64
}

type TrustQualification struct {
	Pubkey       string
	Trusted      bool
	IsSeed       bool
	DistanceHops *int
	Score        *float64
	Rank         *int64
	SourceRunID  *int64
}

type TrustGraphSnapshotRefreshResult struct {
	RowsUpserted int
	SourceRunID  *int64
}

func (s *PostgresStore) RefreshTrustGraphSnapshot(ctx context.Context, maxHops int) (TrustGraphSnapshotRefreshResult, error) {
	if s == nil || s.pool == nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("store is not initialized")
	}
	if maxHops <= 0 {
		maxHops = 3
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("begin trust graph snapshot refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var sourceRunID *int64
	if err := tx.QueryRow(ctx, `
		SELECT id
		FROM trust_runs
		WHERE status = 'succeeded'
		ORDER BY finished_at DESC NULLS LAST, id DESC
		LIMIT 1
	`).Scan(&sourceRunID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("select latest succeeded trust run: %w", err)
		}
		sourceRunID = nil
	}

	if _, err := tx.Exec(ctx, `TRUNCATE trust_graph_snapshot`); err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("truncate trust graph snapshot: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		WITH RECURSIVE reachable(pubkey, hops) AS (
			SELECT pubkey, 0::integer AS hops
			FROM trust_seeds
			WHERE is_active = true
			UNION
			SELECT fe.followed_pubkey, reachable.hops + 1
			FROM reachable
			INNER JOIN follower_edges fe ON fe.follower_pubkey = reachable.pubkey
			WHERE reachable.hops < $1
		),
		agg AS (
			SELECT
				pubkey,
				MIN(hops)::integer AS min_hops,
				BOOL_OR(hops = 0) AS is_seed
			FROM reachable
			GROUP BY pubkey
		)
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed, source_run_id, refreshed_at)
		SELECT pubkey, min_hops, is_seed, $2, now()
		FROM agg
	`, maxHops, sourceRunID)
	if err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("rebuild trust graph snapshot: %w", err)
	}
	if err := refreshTrustedNoteDiscoveryProjectionTx(ctx, tx, 1); err != nil {
		return TrustGraphSnapshotRefreshResult{}, err
	}
	if err := refreshTrustedProfileDiscoveryProjectionTx(ctx, tx, 1); err != nil {
		return TrustGraphSnapshotRefreshResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return TrustGraphSnapshotRefreshResult{}, fmt.Errorf("commit trust graph snapshot refresh: %w", err)
	}
	return TrustGraphSnapshotRefreshResult{
		RowsUpserted: int(tag.RowsAffected()),
		SourceRunID:  sourceRunID,
	}, nil
}

func (s *PostgresStore) GetTrustQualifications(
	ctx context.Context,
	pubkeys []string,
	policy TrustQualificationPolicy,
) (map[string]TrustQualification, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	normalized := normalizePubkeys(pubkeys)
	if len(normalized) == 0 {
		return map[string]TrustQualification{}, nil
	}

	maxHops := policy.MaxHops
	if maxHops < 0 {
		maxHops = 0
	}
	minimumScore := policy.MinimumScore
	if minimumScore < 0 {
		minimumScore = 0
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			requested.pubkey,
			snapshot.min_hops,
			snapshot.is_seed,
			snapshot.source_run_id,
			scores.score,
			scores.rank
		FROM unnest($1::text[]) AS requested(pubkey)
		LEFT JOIN trust_graph_snapshot snapshot ON snapshot.pubkey = requested.pubkey
		LEFT JOIN trust_scores_global scores ON scores.pubkey = requested.pubkey
	`, normalized)
	if err != nil {
		return nil, fmt.Errorf("get trust qualifications: %w", err)
	}
	defer rows.Close()

	out := make(map[string]TrustQualification, len(normalized))
	for rows.Next() {
		var (
			pubkey      string
			minHops     *int
			isSeed      *bool
			sourceRunID *int64
			score       *float64
			rank        *int64
		)
		if err := rows.Scan(&pubkey, &minHops, &isSeed, &sourceRunID, &score, &rank); err != nil {
			return nil, fmt.Errorf("scan trust qualification row: %w", err)
		}

		qualified := TrustQualification{
			Pubkey:       pubkey,
			DistanceHops: minHops,
			Score:        score,
			Rank:         rank,
			SourceRunID:  sourceRunID,
		}
		if isSeed != nil {
			qualified.IsSeed = *isSeed
		}
		if minHops != nil {
			withinHops := maxHops == 0 || *minHops <= maxHops
			meetsScore := minimumScore == 0 || (score != nil && *score >= minimumScore)
			qualified.Trusted = withinHops && meetsScore
		}
		out[pubkey] = qualified
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust qualification rows: %w", err)
	}

	for _, pubkey := range normalized {
		if _, ok := out[pubkey]; ok {
			continue
		}
		out[pubkey] = TrustQualification{Pubkey: pubkey}
	}
	return out, nil
}

func normalizePubkeys(pubkeys []string) []string {
	out := make([]string, 0, len(pubkeys))
	seen := make(map[string]struct{}, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		out = append(out, pubkey)
	}
	return out
}

func (s *PostgresStore) IsTrustedAuthor(ctx context.Context, pubkey string, policy TrustQualificationPolicy) (bool, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return false, fmt.Errorf("pubkey is required")
	}
	rows, err := s.GetTrustQualifications(ctx, []string{pubkey}, policy)
	if err != nil {
		return false, err
	}
	qualification, ok := rows[pubkey]
	if !ok {
		return false, nil
	}
	return qualification.Trusted, nil
}

func (s *PostgresStore) GetTrustSnapshotRefreshedAt(ctx context.Context) (*time.Time, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	var refreshedAt *time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT MAX(refreshed_at)
		FROM trust_graph_snapshot
	`).Scan(&refreshedAt); err != nil {
		return nil, fmt.Errorf("get trust snapshot refreshed_at: %w", err)
	}
	if refreshedAt == nil {
		return nil, nil
	}
	ts := refreshedAt.UTC()
	return &ts, nil
}

func refreshTrustedNoteDiscoveryProjectionTx(ctx context.Context, tx pgx.Tx, derivationVersion int) error {
	var snapshotRefreshedAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT MAX(refreshed_at)
		FROM trust_graph_snapshot
	`).Scan(&snapshotRefreshedAt); err != nil {
		return fmt.Errorf("load trust snapshot timestamp for trusted note projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM trusted_note_discovery_candidates t
		WHERE NOT EXISTS (
			SELECT 1
			FROM note_discovery_stats n
			WHERE n.event_id = t.event_id
		)
	`); err != nil {
		return fmt.Errorf("delete stale trusted note candidates: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_note_discovery_candidates (
			event_id,
			author_pubkey,
			min_hops,
			trust_score,
			source_run_id,
			trust_snapshot_refreshed_at,
			derivation_version,
			projected_at
		)
		SELECT
			n.event_id,
			n.author_pubkey,
			snapshot.min_hops,
			scores.score,
			snapshot.source_run_id,
			$1,
			$2,
			now()
		FROM note_discovery_stats n
		LEFT JOIN trust_graph_snapshot snapshot ON snapshot.pubkey = n.author_pubkey
		LEFT JOIN trust_scores_global scores ON scores.pubkey = n.author_pubkey
		ON CONFLICT (event_id) DO UPDATE
		SET author_pubkey = EXCLUDED.author_pubkey,
		    min_hops = EXCLUDED.min_hops,
		    trust_score = EXCLUDED.trust_score,
		    source_run_id = EXCLUDED.source_run_id,
		    trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`, snapshotRefreshedAt, derivationVersion); err != nil {
		return fmt.Errorf("refresh trusted note discovery projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_discovery_projection_state (
			projection_name,
			trust_snapshot_refreshed_at,
			refreshed_at,
			derivation_version
		)
		VALUES ('trusted_note_discovery_candidates', $1, now(), $2)
		ON CONFLICT (projection_name) DO UPDATE
		SET trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    refreshed_at = now(),
		    derivation_version = EXCLUDED.derivation_version
	`, snapshotRefreshedAt, derivationVersion); err != nil {
		return fmt.Errorf("upsert trusted note projection state: %w", err)
	}
	return nil
}

func refreshTrustedProfileDiscoveryProjectionTx(ctx context.Context, tx pgx.Tx, derivationVersion int) error {
	var snapshotRefreshedAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT MAX(refreshed_at)
		FROM trust_graph_snapshot
	`).Scan(&snapshotRefreshedAt); err != nil {
		return fmt.Errorf("load trust snapshot timestamp for trusted profile projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM trusted_profile_discovery_candidates t
		WHERE NOT EXISTS (
			SELECT 1
			FROM profile_discovery_stats p
			WHERE p.pubkey = t.pubkey
		)
	`); err != nil {
		return fmt.Errorf("delete stale trusted profile candidates: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_profile_discovery_candidates (
			pubkey,
			min_hops,
			trust_score,
			source_run_id,
			trust_snapshot_refreshed_at,
			derivation_version,
			projected_at
		)
		SELECT
			p.pubkey,
			snapshot.min_hops,
			scores.score,
			snapshot.source_run_id,
			$1,
			$2,
			now()
		FROM profile_discovery_stats p
		LEFT JOIN trust_graph_snapshot snapshot ON snapshot.pubkey = p.pubkey
		LEFT JOIN trust_scores_global scores ON scores.pubkey = p.pubkey
		ON CONFLICT (pubkey) DO UPDATE
		SET min_hops = EXCLUDED.min_hops,
		    trust_score = EXCLUDED.trust_score,
		    source_run_id = EXCLUDED.source_run_id,
		    trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`, snapshotRefreshedAt, derivationVersion); err != nil {
		return fmt.Errorf("refresh trusted profile discovery projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_discovery_projection_state (
			projection_name,
			trust_snapshot_refreshed_at,
			refreshed_at,
			derivation_version
		)
		VALUES ('trusted_profile_discovery_candidates', $1, now(), $2)
		ON CONFLICT (projection_name) DO UPDATE
		SET trust_snapshot_refreshed_at = EXCLUDED.trust_snapshot_refreshed_at,
		    refreshed_at = now(),
		    derivation_version = EXCLUDED.derivation_version
	`, snapshotRefreshedAt, derivationVersion); err != nil {
		return fmt.Errorf("upsert trusted profile projection state: %w", err)
	}
	return nil
}
