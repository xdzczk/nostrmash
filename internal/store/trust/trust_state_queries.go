package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

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
