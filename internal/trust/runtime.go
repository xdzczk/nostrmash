package trust

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
)

type Runtime struct {
	pool               *pgxpool.Pool
	enableRedisSync    bool
	enableScoreCompute bool
}

func NewRuntime(pool *pgxpool.Pool, enableRedisSync, enableScoreCompute bool) *Runtime {
	return &Runtime{
		pool:               pool,
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

	payloadRaw, err := json.Marshal(ComputeGlobalScoresPayload{RunID: runID})
	if err != nil {
		return Run{}, fmt.Errorf("encode trust job payload: %w", err)
	}
	var jobID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (job_type, worker_pool, payload, idempotency_key, max_attempts, run_after)
		VALUES ($1, $2, $3, $4, $5, now())
		RETURNING id
	`,
		jobs.JobTypeTrustComputeGlobalScore,
		jobs.WorkerPoolTrust,
		payloadRaw,
		fmt.Sprintf("%s:run:%d", jobs.JobTypeTrustComputeGlobalScore, runID),
		3,
	).Scan(&jobID)
	if err != nil {
		return Run{}, fmt.Errorf("enqueue trust compute job: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET job_id = $1, updated_at = now()
		WHERE id = $2
	`, jobID, runID)
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
		if !r.enableRedisSync {
			return nil
		}
		return nil
	case jobs.JobTypeTrustPromoteRun:
		return nil
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
		err := r.executeGlobalScoresRun(ctx, payload.RunID)
		if err != nil {
			r.markRunFailed(ctx, payload.RunID, err)
		}
		return err
	default:
		return fmt.Errorf("trust job type %q not implemented", job.JobType)
	}
}

func (r *Runtime) executeGlobalScoresRun(ctx context.Context, runID int64) (err error) {
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
	if currentStatus == RunStatusSucceeded {
		return nil
	}

	_, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET status = $1,
		    attempts = attempts + 1,
		    started_at = now(),
		    finished_at = NULL,
		    last_error = NULL,
		    updated_at = now()
		WHERE id = $2
	`, RunStatusRunning, runID)
	if err != nil {
		return fmt.Errorf("mark trust run running: %w", err)
	}

	var edgeCount int64
	if err = tx.QueryRow(ctx, `SELECT COUNT(*) FROM follower_edges`).Scan(&edgeCount); err != nil {
		return fmt.Errorf("count follower edges: %w", err)
	}

	if _, err = tx.Exec(ctx, `DELETE FROM trust_scores_global`); err != nil {
		return fmt.Errorf("clear previous trust scores: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		WITH scored AS (
			SELECT followed_pubkey AS pubkey, COUNT(*)::DOUBLE PRECISION AS score
			FROM follower_edges
			GROUP BY followed_pubkey
		),
		ranked AS (
			SELECT
				pubkey,
				score,
				ROW_NUMBER() OVER (ORDER BY score DESC, pubkey ASC) AS rank
			FROM scored
		)
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
			$1,
			$2,
			$3,
			now(),
			now(),
			now()
		FROM ranked
	`, runID, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion)
	if err != nil {
		return fmt.Errorf("publish trust scores: %w", err)
	}
	scoreRows := tag.RowsAffected()

	_, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET status = $1,
		    input_follower_edges_count = $2,
		    score_rows_published = $3,
		    finished_at = now(),
		    updated_at = now()
		WHERE id = $4
	`, RunStatusSucceeded, edgeCount, scoreRows, runID)
	if err != nil {
		return fmt.Errorf("mark trust run succeeded: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust compute tx: %w", err)
	}
	return nil
}

func (r *Runtime) markRunFailed(ctx context.Context, runID int64, failureErr error) {
	if failureErr == nil {
		return
	}
	msg := failureErr.Error()
	_, _ = r.pool.Exec(ctx, `
		UPDATE trust_runs
		SET status = $1,
		    finished_at = now(),
		    last_error = $2,
		    updated_at = now()
		WHERE id = $3
	`, RunStatusFailed, msg, runID)
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
	return out, nil
}

func (r *Runtime) GetRun(ctx context.Context, runID int64) (Run, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT
			id, derivation_name, target_version, status, job_id, attempts,
			input_follower_edges_count, score_rows_published, redis_snapshot_ref,
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
