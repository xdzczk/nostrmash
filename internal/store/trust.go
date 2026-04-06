package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type TrustGlobalScore struct {
	Pubkey         string
	Score          float64
	Rank           int64
	RunID          int64
	DerivationName string
	TargetVersion  int
	ComputedAt     time.Time
}

type TrustRun struct {
	ID                 int64
	DerivationName     string
	TargetVersion      int
	Status             string
	JobID              *int64
	Attempts           int
	InputFollowerEdges int64
	ScoreRowsPublished int64
	RedisSnapshotRef   *string
	StartedAt          *time.Time
	FinishedAt         *time.Time
	LastError          *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func (s *PostgresStore) GetTrustScore(ctx context.Context, pubkey string) (TrustGlobalScore, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return TrustGlobalScore{}, fmt.Errorf("pubkey is required")
	}
	row := s.pool.QueryRow(ctx, `
		SELECT pubkey, score, rank, run_id, derivation_name, target_version, computed_at
		FROM trust_scores_global
		WHERE pubkey = $1
	`, pubkey)
	var out TrustGlobalScore
	if err := row.Scan(
		&out.Pubkey,
		&out.Score,
		&out.Rank,
		&out.RunID,
		&out.DerivationName,
		&out.TargetVersion,
		&out.ComputedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TrustGlobalScore{}, ErrNotFound
		}
		return TrustGlobalScore{}, fmt.Errorf("get trust score: %w", err)
	}
	out.ComputedAt = out.ComputedAt.UTC()
	return out, nil
}

func (s *PostgresStore) ListTopTrustedPubkeys(ctx context.Context, limit int) ([]TrustGlobalScore, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, score, rank, run_id, derivation_name, target_version, computed_at
		FROM trust_scores_global
		ORDER BY rank ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list top trust scores: %w", err)
	}
	defer rows.Close()
	out := make([]TrustGlobalScore, 0, limit)
	for rows.Next() {
		var item TrustGlobalScore
		if err := rows.Scan(
			&item.Pubkey,
			&item.Score,
			&item.Rank,
			&item.RunID,
			&item.DerivationName,
			&item.TargetVersion,
			&item.ComputedAt,
		); err != nil {
			return nil, fmt.Errorf("scan top trust score: %w", err)
		}
		item.ComputedAt = item.ComputedAt.UTC()
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read top trust scores: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetTrustRun(ctx context.Context, runID int64) (TrustRun, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, derivation_name, target_version, status, job_id, attempts,
		       input_follower_edges_count, score_rows_published, redis_snapshot_ref,
		       started_at, finished_at, last_error, created_at, updated_at
		FROM trust_runs
		WHERE id = $1
	`, runID)
	out, err := scanTrustRun(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TrustRun{}, ErrNotFound
		}
		return TrustRun{}, err
	}
	return out, nil
}

func (s *PostgresStore) ListTrustRuns(ctx context.Context, limit int) ([]TrustRun, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, derivation_name, target_version, status, job_id, attempts,
		       input_follower_edges_count, score_rows_published, redis_snapshot_ref,
		       started_at, finished_at, last_error, created_at, updated_at
		FROM trust_runs
		ORDER BY created_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list trust runs: %w", err)
	}
	defer rows.Close()
	out := make([]TrustRun, 0, limit)
	for rows.Next() {
		run, err := scanTrustRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust runs: %w", err)
	}
	return out, nil
}

func scanTrustRun(row interface{ Scan(dest ...any) error }) (TrustRun, error) {
	var out TrustRun
	if err := row.Scan(
		&out.ID,
		&out.DerivationName,
		&out.TargetVersion,
		&out.Status,
		&out.JobID,
		&out.Attempts,
		&out.InputFollowerEdges,
		&out.ScoreRowsPublished,
		&out.RedisSnapshotRef,
		&out.StartedAt,
		&out.FinishedAt,
		&out.LastError,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		return TrustRun{}, fmt.Errorf("scan trust run: %w", err)
	}
	out.CreatedAt = out.CreatedAt.UTC()
	out.UpdatedAt = out.UpdatedAt.UTC()
	if out.StartedAt != nil {
		ts := out.StartedAt.UTC()
		out.StartedAt = &ts
	}
	if out.FinishedAt != nil {
		ts := out.FinishedAt.UTC()
		out.FinishedAt = &ts
	}
	return out, nil
}
