package api

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
)

type adminTrustRunResponse struct {
	ID                 int64      `json:"id"`
	DerivationName     string     `json:"derivation_name"`
	TargetVersion      int        `json:"target_version"`
	Status             string     `json:"status"`
	JobID              *int64     `json:"job_id,omitempty"`
	Attempts           int        `json:"attempts"`
	InputFollowerEdges int64      `json:"input_follower_edges_count"`
	ScoreRowsPublished int64      `json:"score_rows_published"`
	RedisSnapshotRef   *string    `json:"redis_snapshot_ref,omitempty"`
	CurrentPhase       *string    `json:"current_phase,omitempty"`
	SyncJobID          *int64     `json:"sync_job_id,omitempty"`
	ComputeJobID       *int64     `json:"compute_job_id,omitempty"`
	PromoteJobID       *int64     `json:"promote_job_id,omitempty"`
	PhaseStartedAt     *time.Time `json:"phase_started_at,omitempty"`
	PhaseFinishedAt    *time.Time `json:"phase_finished_at,omitempty"`
	PhaseLastError     *string    `json:"phase_last_error,omitempty"`
	StartedAt          *time.Time `json:"started_at,omitempty"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
	LastError          *string    `json:"last_error,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

type adminTrustScoreResponse struct {
	Pubkey         string    `json:"pubkey"`
	Score          float64   `json:"score"`
	Rank           int64     `json:"rank"`
	RunID          int64     `json:"run_id"`
	DerivationName string    `json:"derivation_name"`
	TargetVersion  int       `json:"target_version"`
	ComputedAt     time.Time `json:"computed_at"`
}

func (s *adminService) GetTrustRuns(ctx context.Context, limit int) ([]adminTrustRunResponse, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, derivation_name, target_version, status, job_id, attempts,
		       input_follower_edges_count, score_rows_published, redis_snapshot_ref,
		       current_phase, sync_job_id, compute_job_id, promote_job_id,
		       phase_started_at, phase_finished_at, phase_last_error,
		       started_at, finished_at, last_error, created_at, updated_at
		FROM trust_runs
		ORDER BY created_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list trust runs: %w", err)
	}
	defer rows.Close()
	out := make([]adminTrustRunResponse, 0, limit)
	for rows.Next() {
		var row store.TrustRun
		if err := rows.Scan(
			&row.ID,
			&row.DerivationName,
			&row.TargetVersion,
			&row.Status,
			&row.JobID,
			&row.Attempts,
			&row.InputFollowerEdges,
			&row.ScoreRowsPublished,
			&row.RedisSnapshotRef,
			&row.CurrentPhase,
			&row.SyncJobID,
			&row.ComputeJobID,
			&row.PromoteJobID,
			&row.PhaseStartedAt,
			&row.PhaseFinishedAt,
			&row.PhaseLastError,
			&row.StartedAt,
			&row.FinishedAt,
			&row.LastError,
			&row.CreatedAt,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan trust run: %w", err)
		}
		out = append(out, asAdminTrustRun(row))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust run rows: %w", err)
	}
	return out, nil
}

func (s *adminService) GetTrustRun(ctx context.Context, runID int64) (adminTrustRunResponse, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, derivation_name, target_version, status, job_id, attempts,
		       input_follower_edges_count, score_rows_published, redis_snapshot_ref,
		       current_phase, sync_job_id, compute_job_id, promote_job_id,
		       phase_started_at, phase_finished_at, phase_last_error,
		       started_at, finished_at, last_error, created_at, updated_at
		FROM trust_runs
		WHERE id = $1
	`, runID)
	var out store.TrustRun
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
		&out.CurrentPhase,
		&out.SyncJobID,
		&out.ComputeJobID,
		&out.PromoteJobID,
		&out.PhaseStartedAt,
		&out.PhaseFinishedAt,
		&out.PhaseLastError,
		&out.StartedAt,
		&out.FinishedAt,
		&out.LastError,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		return adminTrustRunResponse{}, fmt.Errorf("get trust run: %w", err)
	}
	return asAdminTrustRun(out), nil
}

func (s *adminService) TriggerTrustRun(ctx context.Context) (adminTrustRunResponse, error) {
	if s.trust == nil {
		return adminTrustRunResponse{}, fmt.Errorf("trust runtime is not configured")
	}
	run, err := s.trust.TriggerGlobalRun(ctx)
	if err != nil {
		return adminTrustRunResponse{}, err
	}
	return adminTrustRunResponse{
		ID:             run.ID,
		DerivationName: run.DerivationName,
		TargetVersion:  run.TargetVersion,
		Status:         run.Status,
		JobID:          run.JobID,
		Attempts:       run.Attempts,
		StartedAt:      run.StartedAt,
		FinishedAt:     run.FinishedAt,
		LastError:      run.LastError,
		CreatedAt:      run.CreatedAt,
		UpdatedAt:      run.UpdatedAt,
	}, nil
}

func (s *adminService) GetTopTrustScores(ctx context.Context, limit int) ([]adminTrustScoreResponse, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, score, rank, run_id, derivation_name, target_version, computed_at
		FROM trust_scores_global
		ORDER BY rank ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list trust scores: %w", err)
	}
	defer rows.Close()
	out := make([]adminTrustScoreResponse, 0, limit)
	for rows.Next() {
		var item adminTrustScoreResponse
		if err := rows.Scan(
			&item.Pubkey,
			&item.Score,
			&item.Rank,
			&item.RunID,
			&item.DerivationName,
			&item.TargetVersion,
			&item.ComputedAt,
		); err != nil {
			return nil, fmt.Errorf("scan trust score: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust score rows: %w", err)
	}
	return out, nil
}

func asAdminTrustRun(in store.TrustRun) adminTrustRunResponse {
	var startedAt *time.Time
	if in.StartedAt != nil {
		ts := in.StartedAt.UTC()
		startedAt = &ts
	}
	var finishedAt *time.Time
	if in.FinishedAt != nil {
		ts := in.FinishedAt.UTC()
		finishedAt = &ts
	}
	var phaseStartedAt *time.Time
	if in.PhaseStartedAt != nil {
		ts := in.PhaseStartedAt.UTC()
		phaseStartedAt = &ts
	}
	var phaseFinishedAt *time.Time
	if in.PhaseFinishedAt != nil {
		ts := in.PhaseFinishedAt.UTC()
		phaseFinishedAt = &ts
	}
	return adminTrustRunResponse{
		ID:                 in.ID,
		DerivationName:     in.DerivationName,
		TargetVersion:      in.TargetVersion,
		Status:             in.Status,
		JobID:              in.JobID,
		Attempts:           in.Attempts,
		InputFollowerEdges: in.InputFollowerEdges,
		ScoreRowsPublished: in.ScoreRowsPublished,
		RedisSnapshotRef:   in.RedisSnapshotRef,
		CurrentPhase:       in.CurrentPhase,
		SyncJobID:          in.SyncJobID,
		ComputeJobID:       in.ComputeJobID,
		PromoteJobID:       in.PromoteJobID,
		PhaseStartedAt:     phaseStartedAt,
		PhaseFinishedAt:    phaseFinishedAt,
		PhaseLastError:     in.PhaseLastError,
		StartedAt:          startedAt,
		FinishedAt:         finishedAt,
		LastError:          in.LastError,
		CreatedAt:          in.CreatedAt.UTC(),
		UpdatedAt:          in.UpdatedAt.UTC(),
	}
}
