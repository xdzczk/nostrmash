package trust

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/jobs"
)

func enqueueTrustJobTx(ctx context.Context, tx pgx.Tx, jobType string, payload []byte, idempotencyKey string) (int64, error) {
	var jobID int64
	err := tx.QueryRow(ctx, `
		INSERT INTO jobs (job_type, worker_pool, payload, idempotency_key, max_attempts, run_after)
		VALUES ($1, $2, $3, $4, $5, now())
		RETURNING id
	`, jobType, jobs.WorkerPoolForJobType(jobType), payload, idempotencyKey, 3).Scan(&jobID)
	if err != nil {
		return 0, err
	}
	return jobID, nil
}

func (r *Runtime) markRunFailed(ctx context.Context, runID int64, phase string, failureErr error) {
	if failureErr == nil {
		return
	}
	msg := failureErr.Error()
	_, _ = r.pool.Exec(ctx, `
		UPDATE trust_runs
		SET status = $1,
		    current_phase = NULLIF($2, ''),
		    phase_finished_at = now(),
		    phase_last_error = $3,
		    finished_at = now(),
		    last_error = $3,
		    updated_at = now()
		WHERE id = $4
	`, RunStatusFailed, strings.TrimSpace(phase), msg, runID)
}

func scanRunRow(row interface{ Scan(dest ...any) error }) (Run, error) {
	var out Run
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
		return Run{}, err
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
	if out.PhaseStartedAt != nil {
		ts := out.PhaseStartedAt.UTC()
		out.PhaseStartedAt = &ts
	}
	if out.PhaseFinishedAt != nil {
		ts := out.PhaseFinishedAt.UTC()
		out.PhaseFinishedAt = &ts
	}
	return out, nil
}

func (r *Runtime) GetRun(ctx context.Context, runID int64) (Run, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT
			id, derivation_name, target_version, status, job_id, attempts,
			input_follower_edges_count, score_rows_published, redis_snapshot_ref,
			current_phase, sync_job_id, compute_job_id, promote_job_id,
			phase_started_at, phase_finished_at, phase_last_error,
			started_at, finished_at, last_error, created_at, updated_at
		FROM trust_runs
		WHERE id = $1
	`, runID)
	run, err := scanRunRow(row)
	if err != nil {
		return Run{}, fmt.Errorf("get trust run: %w", err)
	}
	return run, nil
}
