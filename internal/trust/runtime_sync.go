package trust

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

func (r *Runtime) executeRedisSyncRun(ctx context.Context, runID int64) error {
	started := time.Now()
	snapshotRef := "postgres-only"
	edgeCount := int64(0)

	if r.enableRedisSync {
		result, err := r.syncGraphToRedis(ctx, runID)
		if err != nil {
			metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustSyncGraphRedis, "error", time.Since(started))
			metrics.ObserveTrustPhaseDuration(RunPhaseSync, "error", time.Since(started))
			return err
		}
		snapshotRef = result.SnapshotRef
		edgeCount = result.EdgeCount
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin trust redis sync tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM trust_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&currentStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("trust run %d not found", runID)
		}
		return fmt.Errorf("lock trust run for redis sync: %w", err)
	}
	if currentStatus == RunStatusSucceeded || currentStatus == RunStatusFailed {
		metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustSyncGraphRedis, "skipped", time.Since(started))
		metrics.ObserveTrustPhaseDuration(RunPhaseSync, "skipped", time.Since(started))
		return nil
	}

	var followerEdges int64
	if edgeCount > 0 {
		followerEdges = edgeCount
	} else {
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM follower_edges`).Scan(&followerEdges); err != nil {
			return fmt.Errorf("count follower edges during trust redis sync: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET status = $1,
		    attempts = attempts + 1,
		    started_at = now(),
		    finished_at = NULL,
		    last_error = NULL,
		    current_phase = $3,
		    phase_started_at = now(),
		    phase_finished_at = NULL,
		    phase_last_error = NULL,
		    redis_snapshot_ref = $4,
		    input_follower_edges_count = $5,
		    updated_at = now()
		WHERE id = $2
	`, RunStatusRunning, runID, RunPhaseSync, snapshotRef, followerEdges)
	if err != nil {
		return fmt.Errorf("mark trust run running after redis sync: %w", err)
	}

	payload, err := json.Marshal(ComputeGlobalScoresPayload{
		RunID:            runID,
		RedisSnapshotRef: snapshotRef,
	})
	if err != nil {
		return fmt.Errorf("encode trust compute payload: %w", err)
	}
	computeJobID, err := enqueueTrustJobTx(
		ctx,
		tx,
		jobs.JobTypeTrustComputeGlobalScore,
		payload,
		fmt.Sprintf("%s:run:%d:%s", jobs.JobTypeTrustComputeGlobalScore, runID, snapshotRef),
	)
	if err != nil {
		return fmt.Errorf("enqueue trust compute job after sync: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE trust_runs
		SET compute_job_id = $1,
		    job_id = $1,
		    updated_at = now()
		WHERE id = $2
	`, computeJobID, runID); err != nil {
		return fmt.Errorf("set trust run compute job id: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust redis sync tx: %w", err)
	}
	metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustSyncGraphRedis, "success", time.Since(started))
	metrics.ObserveTrustPhaseDuration(RunPhaseSync, "success", time.Since(started))
	return nil
}
