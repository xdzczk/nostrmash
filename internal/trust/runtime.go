package trust

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

type Runtime struct {
	pool               *pgxpool.Pool
	redis              redisClient
	redisKeyPrefix     string
	enableRedisSync    bool
	enableScoreCompute bool
}

func NewRuntime(pool *pgxpool.Pool, enableRedisSync, enableScoreCompute bool) *Runtime {
	return NewRuntimeWithRedis(pool, nil, "nostrmash", enableRedisSync, enableScoreCompute)
}

func NewRuntimeWithRedis(
	pool *pgxpool.Pool,
	redis redisClient,
	redisKeyPrefix string,
	enableRedisSync, enableScoreCompute bool,
) *Runtime {
	return &Runtime{
		pool:               pool,
		redis:              redis,
		redisKeyPrefix:     strings.TrimSpace(redisKeyPrefix),
		enableRedisSync:    enableRedisSync,
		enableScoreCompute: enableScoreCompute,
	}
}

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

func (r *Runtime) executeGlobalScoresRun(ctx context.Context, runID int64, snapshotRef string) (err error) {
	started := time.Now()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin trust compute tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM trust_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&currentStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("trust run %d not found", runID)
		}
		return fmt.Errorf("lock trust run: %w", err)
	}
	if currentStatus == RunStatusSucceeded || currentStatus == RunStatusFailed {
		metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustComputeGlobalScore, "skipped", time.Since(started))
		metrics.ObserveTrustPhaseDuration(RunPhaseCompute, "skipped", time.Since(started))
		return nil
	}

	if strings.TrimSpace(snapshotRef) == "" {
		err = tx.QueryRow(ctx, `SELECT COALESCE(redis_snapshot_ref, '') FROM trust_runs WHERE id = $1`, runID).Scan(&snapshotRef)
		if err != nil {
			return fmt.Errorf("load trust run redis snapshot ref: %w", err)
		}
	}

	var adjacency map[string][]string
	var nodeSet map[string]struct{}
	if r.enableRedisSync && r.redis != nil && strings.TrimSpace(snapshotRef) != "" && snapshotRef != "postgres-only" {
		adjacency, nodeSet, err = r.loadAdjacencyFromRedis(ctx, runID, snapshotRef)
		if err != nil {
			return err
		}
	} else {
		adjacency, nodeSet, err = r.loadAdjacencyFromPostgres(ctx)
		if err != nil {
			return err
		}
	}

	if _, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET current_phase = $1,
		    phase_started_at = now(),
		    phase_finished_at = NULL,
		    phase_last_error = NULL,
		    updated_at = now()
		WHERE id = $2
	`, RunPhaseCompute, runID); err != nil {
		return fmt.Errorf("mark trust run compute phase: %w", err)
	}

	if _, err = tx.Exec(ctx, `DELETE FROM trust_scores_global_stage WHERE run_id = $1`, runID); err != nil {
		return fmt.Errorf("clear previous trust score staging rows: %w", err)
	}

	ranked := computeIterativeGlobalRank(adjacency, nodeSet)
	if len(ranked) > 0 {
		rows := make([][]any, 0, len(ranked))
		for i, item := range ranked {
			rows = append(rows, []any{
				runID,
				item.Pubkey,
				item.Score,
				i + 1,
				derivation.DerivationTrustScoresGlobal,
				derivation.TrustScoresGlobalVersion,
			})
		}
		_, err = tx.CopyFrom(
			ctx,
			pgx.Identifier{"trust_scores_global_stage"},
			[]string{"run_id", "pubkey", "score", "rank", "derivation_name", "target_version"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("write trust score staging rows by copy: %w", err)
		}
	}

	scoreRows := int64(len(ranked))
	followerEdges := int64(0)
	if err = tx.QueryRow(ctx, `SELECT COUNT(*) FROM follower_edges`).Scan(&followerEdges); err != nil {
		return fmt.Errorf("count follower edges: %w", err)
	}

	payload, err := json.Marshal(PromoteRunPayload{
		RunID:            runID,
		RedisSnapshotRef: snapshotRef,
	})
	if err != nil {
		return fmt.Errorf("encode trust promote payload: %w", err)
	}
	promoteJobID, err := enqueueTrustJobTx(
		ctx,
		tx,
		jobs.JobTypeTrustPromoteRun,
		payload,
		fmt.Sprintf("%s:run:%d:%s", jobs.JobTypeTrustPromoteRun, runID, snapshotRef),
	)
	if err != nil {
		return fmt.Errorf("enqueue trust promote job: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET status = $1,
		    input_follower_edges_count = $2,
		    score_rows_published = $3,
		    redis_snapshot_ref = NULLIF($4, ''),
		    phase_finished_at = now(),
		    promote_job_id = $5,
		    job_id = $5,
		    updated_at = now()
		WHERE id = $6
	`, RunStatusRunning, followerEdges, scoreRows, snapshotRef, promoteJobID, runID)
	if err != nil {
		return fmt.Errorf("mark trust run computed: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust compute tx: %w", err)
	}
	metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustComputeGlobalScore, "success", time.Since(started))
	metrics.ObserveTrustPhaseDuration(RunPhaseCompute, "success", time.Since(started))
	return nil
}

func (r *Runtime) executePromoteRun(ctx context.Context, runID int64, snapshotRef string) error {
	started := time.Now()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin trust promote tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM trust_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("trust run %d not found", runID)
		}
		return fmt.Errorf("lock trust run for promote: %w", err)
	}
	if status == RunStatusSucceeded || status == RunStatusFailed {
		metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustPromoteRun, "skipped", time.Since(started))
		metrics.ObserveTrustPhaseDuration(RunPhasePromote, "skipped", time.Since(started))
		return nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE trust_runs
		SET current_phase = $1,
		    phase_started_at = now(),
		    phase_finished_at = NULL,
		    phase_last_error = NULL,
		    updated_at = now()
		WHERE id = $2
	`, RunPhasePromote, runID); err != nil {
		return fmt.Errorf("mark trust run promote phase: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM trust_scores_global`); err != nil {
		return fmt.Errorf("clear previous trust global scores: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO trust_scores_global (
			pubkey,
			score,
			rank,
			run_id,
			derivation_name,
			target_version,
			computed_at,
			created_at,
			updated_at
		)
		SELECT
			pubkey,
			score,
			rank,
			run_id,
			derivation_name,
			target_version,
			computed_at,
			now(),
			now()
		FROM trust_scores_global_stage
		WHERE run_id = $1
		ORDER BY rank ASC
	`, runID)
	if err != nil {
		return fmt.Errorf("publish staged trust global scores: %w", err)
	}
	metrics.AddTrustScoreRowsPublished(tag.RowsAffected())

	_, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET status = $1,
		    redis_snapshot_ref = COALESCE(NULLIF($2, ''), redis_snapshot_ref),
		    phase_finished_at = now(),
		    finished_at = now(),
		    updated_at = now()
		WHERE id = $3
	`, RunStatusSucceeded, snapshotRef, runID)
	if err != nil {
		return fmt.Errorf("mark trust run succeeded in promote: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust promote tx: %w", err)
	}
	metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustPromoteRun, "success", time.Since(started))
	metrics.ObserveTrustPhaseDuration(RunPhasePromote, "success", time.Since(started))
	return nil
}

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
