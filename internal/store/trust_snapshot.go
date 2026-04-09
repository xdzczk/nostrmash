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

type TrustState struct {
	Pubkey       string
	Score        *float64
	Qualified    bool
	Tier         string
	HopDistance  *int
	HopBucket    string
	Rank         *int64
	ComputedAt   *time.Time
	GenerationID *int64
	IsSeed       bool
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
	states, err := s.GetTrustStates(ctx, normalized)
	if err != nil {
		return nil, err
	}
	out := make(map[string]TrustQualification, len(normalized))
	for _, pubkey := range normalized {
		state := states[pubkey]
		out[pubkey] = trustQualificationFromState(state, policy)
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

func (s *PostgresStore) GetTrustState(ctx context.Context, pubkey string) (TrustState, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return TrustState{}, fmt.Errorf("pubkey is required")
	}
	states, err := s.GetTrustStates(ctx, []string{pubkey})
	if err != nil {
		return TrustState{}, err
	}
	state, ok := states[pubkey]
	if !ok || !trustStateKnown(state) {
		return TrustState{}, ErrNotFound
	}
	return state, nil
}

func (s *PostgresStore) GetTrustStates(ctx context.Context, pubkeys []string) (map[string]TrustState, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	normalized := normalizePubkeys(pubkeys)
	if len(normalized) == 0 {
		return map[string]TrustState{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			requested.pubkey,
			snapshot.min_hops,
			snapshot.is_seed,
			COALESCE(snapshot.source_run_id, scores.run_id),
			scores.score,
			scores.rank,
			scores.computed_at,
			snapshot.refreshed_at
		FROM unnest($1::text[]) AS requested(pubkey)
		LEFT JOIN trust_graph_snapshot snapshot ON snapshot.pubkey = requested.pubkey
		LEFT JOIN trust_scores_global scores ON scores.pubkey = requested.pubkey
	`, normalized)
	if err != nil {
		return nil, fmt.Errorf("get trust states: %w", err)
	}
	defer rows.Close()

	out := make(map[string]TrustState, len(normalized))
	for rows.Next() {
		var (
			pubkey          string
			hopDistance     *int
			isSeed          *bool
			generationID    *int64
			score           *float64
			rank            *int64
			scoreComputedAt *time.Time
			refreshedAt     *time.Time
		)
		if err := rows.Scan(
			&pubkey,
			&hopDistance,
			&isSeed,
			&generationID,
			&score,
			&rank,
			&scoreComputedAt,
			&refreshedAt,
		); err != nil {
			return nil, fmt.Errorf("scan trust state row: %w", err)
		}
		state := TrustState{
			Pubkey:       pubkey,
			Score:        score,
			HopDistance:  hopDistance,
			HopBucket:    trustHopBucket(hopDistance),
			Rank:         rank,
			GenerationID: generationID,
		}
		if isSeed != nil {
			state.IsSeed = *isSeed
		}
		state.Qualified = trustStateQualified(state)
		state.Tier = trustTierFromState(state)
		if scoreComputedAt != nil {
			ts := scoreComputedAt.UTC()
			state.ComputedAt = &ts
		} else if refreshedAt != nil {
			ts := refreshedAt.UTC()
			state.ComputedAt = &ts
		}
		out[pubkey] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust state rows: %w", err)
	}
	for _, pubkey := range normalized {
		if _, ok := out[pubkey]; ok {
			continue
		}
		out[pubkey] = TrustState{
			Pubkey:    pubkey,
			Tier:      "unknown",
			HopBucket: "unknown",
		}
	}
	return out, nil
}

func trustQualificationFromState(state TrustState, policy TrustQualificationPolicy) TrustQualification {
	policy = normalizeTrustPolicy(policy)
	return TrustQualification{
		Pubkey:       state.Pubkey,
		Trusted:      trustStateTrusted(state, policy),
		IsSeed:       state.IsSeed,
		DistanceHops: state.HopDistance,
		Score:        state.Score,
		Rank:         state.Rank,
		SourceRunID:  state.GenerationID,
	}
}

func normalizeTrustPolicy(policy TrustQualificationPolicy) TrustQualificationPolicy {
	if policy.MaxHops < 0 {
		policy.MaxHops = 0
	}
	if policy.MinimumScore < 0 {
		policy.MinimumScore = 0
	}
	return policy
}

func trustStateTrusted(state TrustState, policy TrustQualificationPolicy) bool {
	if !trustStateQualified(state) {
		return false
	}
	if state.HopDistance != nil && policy.MaxHops > 0 && *state.HopDistance > policy.MaxHops {
		return false
	}
	if policy.MinimumScore > 0 {
		if state.Score == nil || *state.Score < policy.MinimumScore {
			return false
		}
	}
	return true
}

func trustStateQualified(state TrustState) bool {
	return state.IsSeed || state.HopDistance != nil || state.Score != nil
}

func trustStateKnown(state TrustState) bool {
	return trustStateQualified(state) || state.GenerationID != nil || state.ComputedAt != nil || state.Rank != nil
}

func trustHopBucket(hops *int) string {
	if hops == nil {
		return "unknown"
	}
	switch {
	case *hops <= 0:
		return "0"
	case *hops == 1:
		return "1"
	case *hops == 2:
		return "2"
	case *hops == 3:
		return "3"
	default:
		return "4_plus"
	}
}

func trustTierFromState(state TrustState) string {
	if state.IsSeed {
		return "seed"
	}
	if state.HopDistance == nil {
		return "unknown"
	}
	switch {
	case *state.HopDistance <= 1:
		return "core"
	case *state.HopDistance <= 3:
		return "near"
	default:
		return "outer"
	}
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
