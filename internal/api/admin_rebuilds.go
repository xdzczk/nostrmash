package api

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
)

type adminRebuildRunResponse struct {
	ID             int64                             `json:"id"`
	DerivationName string                            `json:"derivation_name"`
	TargetVersion  int                               `json:"target_version"`
	Scope          derivation.ProjectionRebuildScope `json:"scope"`
	Status         string                            `json:"status"`
	JobID          *int64                            `json:"job_id,omitempty"`
	Attempts       int                               `json:"attempts"`
	StartedAt      *time.Time                        `json:"started_at,omitempty"`
	FinishedAt     *time.Time                        `json:"finished_at,omitempty"`
	LastError      *string                           `json:"last_error,omitempty"`
}

func (s *adminService) GetRebuilds(ctx context.Context, limit int) ([]adminRebuildRunResponse, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id,
			derivation_name,
			target_version,
			scope_type,
			scope_event_id,
			scope_pubkey,
			scope_start_created_at,
			scope_end_created_at,
			status,
			job_id,
			attempts,
			started_at,
			finished_at,
			last_error
		FROM projection_rebuild_runs
		ORDER BY created_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list rebuild runs: %w", err)
	}
	defer rows.Close()
	out := make([]adminRebuildRunResponse, 0, limit)
	for rows.Next() {
		run, err := scanAdminRebuildRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rebuild rows: %w", err)
	}
	return out, nil
}

func (s *adminService) TriggerRebuild(ctx context.Context, params derivation.TriggerProjectionRebuildParams) (adminRebuildRunResponse, error) {
	run, err := s.derivation.TriggerProjectionRebuild(ctx, params)
	if err != nil {
		return adminRebuildRunResponse{}, err
	}
	return asAdminRebuildRun(run), nil
}

func scanAdminRebuildRun(row interface{ Scan(dest ...any) error }) (adminRebuildRunResponse, error) {
	var out adminRebuildRunResponse
	var scopeEventID *string
	var scopePubkey *string
	err := row.Scan(
		&out.ID,
		&out.DerivationName,
		&out.TargetVersion,
		&out.Scope.Type,
		&scopeEventID,
		&scopePubkey,
		&out.Scope.StartCreatedAt,
		&out.Scope.EndCreatedAt,
		&out.Status,
		&out.JobID,
		&out.Attempts,
		&out.StartedAt,
		&out.FinishedAt,
		&out.LastError,
	)
	if err != nil {
		return out, fmt.Errorf("scan rebuild run: %w", err)
	}
	if scopeEventID != nil {
		out.Scope.EventID = *scopeEventID
	}
	if scopePubkey != nil {
		out.Scope.Pubkey = *scopePubkey
	}
	if out.StartedAt != nil {
		utc := out.StartedAt.UTC()
		out.StartedAt = &utc
	}
	if out.FinishedAt != nil {
		utc := out.FinishedAt.UTC()
		out.FinishedAt = &utc
	}
	return out, nil
}

func asAdminRebuildRun(run derivation.ProjectionRebuildRun) adminRebuildRunResponse {
	return adminRebuildRunResponse{
		ID:             run.ID,
		DerivationName: run.DerivationName,
		TargetVersion:  run.TargetVersion,
		Scope:          run.Scope,
		Status:         run.Status,
		JobID:          run.JobID,
		Attempts:       run.Attempts,
		StartedAt:      run.StartedAt,
		FinishedAt:     run.FinishedAt,
		LastError:      run.LastError,
	}
}
