package trust

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
)

func (r *Runtime) TriggerGlobalRun(ctx context.Context) (Run, error) {
	if r == nil || r.pool == nil {
		return Run{}, fmt.Errorf("trust runtime is not initialized")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Run{}, fmt.Errorf("begin trust run tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var runID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO trust_runs (derivation_name, target_version, status)
		VALUES ($1, $2, $3)
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion, RunStatusPending).Scan(&runID)
	if err != nil {
		return Run{}, fmt.Errorf("insert trust run: %w", err)
	}

	payloadRaw, err := json.Marshal(SyncGraphRedisPayload{RunID: runID})
	if err != nil {
		return Run{}, fmt.Errorf("encode trust job payload: %w", err)
	}
	jobID, err := enqueueTrustJobTx(
		ctx,
		tx,
		jobs.JobTypeTrustSyncGraphRedis,
		payloadRaw,
		fmt.Sprintf("%s:run:%d", jobs.JobTypeTrustSyncGraphRedis, runID),
	)
	if err != nil {
		return Run{}, fmt.Errorf("enqueue trust sync job: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET job_id = $1,
		    sync_job_id = $1,
		    current_phase = $2,
		    phase_started_at = now(),
		    phase_finished_at = NULL,
		    phase_last_error = NULL,
		    updated_at = now()
		WHERE id = $3
	`, jobID, RunPhaseSync, runID)
	if err != nil {
		return Run{}, fmt.Errorf("attach trust run job: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Run{}, fmt.Errorf("commit trust run trigger: %w", err)
	}
	return r.GetRun(ctx, runID)
}

func (r *Runtime) ProcessJob(ctx context.Context, job jobs.Job) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("trust runtime is not initialized")
	}
	switch strings.TrimSpace(job.JobType) {
	case jobs.JobTypeTrustSyncGraphRedis:
		var payload SyncGraphRedisPayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return fmt.Errorf("decode trust redis sync payload: %w", err)
		}
		if payload.RunID <= 0 {
			return fmt.Errorf("run_id is required in redis sync payload")
		}
		err := r.executeRedisSyncRun(ctx, payload.RunID)
		if err != nil {
			r.markRunFailed(ctx, payload.RunID, RunPhaseSync, err)
		}
		return err
	case jobs.JobTypeTrustPromoteRun:
		var payload PromoteRunPayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return fmt.Errorf("decode trust promote payload: %w", err)
		}
		if payload.RunID <= 0 {
			return fmt.Errorf("run_id is required in promote payload")
		}
		err := r.executePromoteRun(ctx, payload.RunID, payload.RedisSnapshotRef)
		if err != nil {
			r.markRunFailed(ctx, payload.RunID, RunPhasePromote, err)
		}
		return err
	case jobs.JobTypeTrustComputeGlobalScore:
		if !r.enableScoreCompute {
			return fmt.Errorf("trust score compute is disabled")
		}
		var payload ComputeGlobalScoresPayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return fmt.Errorf("decode trust global score payload: %w", err)
		}
		if payload.RunID <= 0 {
			return fmt.Errorf("run_id is required in payload")
		}
		err := r.executeGlobalScoresRun(ctx, payload.RunID, payload.RedisSnapshotRef)
		if err != nil {
			r.markRunFailed(ctx, payload.RunID, RunPhaseCompute, err)
		}
		return err
	default:
		return fmt.Errorf("trust job type %q not implemented", job.JobType)
	}
}
