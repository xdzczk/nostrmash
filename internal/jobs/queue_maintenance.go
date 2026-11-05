package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

func (q *Queue) GetJobByID(ctx context.Context, jobID int64) (*Job, error) {
	if q == nil || q.pool == nil {
		return nil, fmt.Errorf("queue is not initialized")
	}

	row := q.pool.QueryRow(ctx, `
		SELECT id, job_type, worker_pool, payload, idempotency_key, status, attempts, max_attempts,
		       run_after, locked_at, locked_by, last_error, created_at, updated_at, finished_at
		FROM jobs
		WHERE id = $1
	`,
		jobID,
	)
	job, err := scanJobRow(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get job by id: %w", err)
	}
	return job, nil
}

func (q *Queue) RecoverStaleRunningJobs(
	ctx context.Context,
	workerPool string,
	olderThan time.Time,
	limit int,
) (result RecoveryResult, err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveQueueOperation("recover_stale_running_jobs", queueResultFromErr(err), time.Since(started))
	}()
	if q == nil || q.pool == nil {
		return result, fmt.Errorf("queue is not initialized")
	}
	workerPool = strings.TrimSpace(workerPool)
	if workerPool == "" {
		return result, fmt.Errorf("worker pool is required")
	}
	if olderThan.IsZero() {
		return result, fmt.Errorf("olderThan is required")
	}
	if limit <= 0 {
		return result, fmt.Errorf("limit must be > 0")
	}

	rows, err := q.pool.Query(ctx, `
		WITH stale AS (
			SELECT id
			FROM jobs
			WHERE status = $1
			  AND worker_pool = $2
			  AND locked_at < $3
			ORDER BY locked_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $4
		),
		recovered AS (
			UPDATE jobs j
			SET attempts = j.attempts + 1,
			    status = CASE WHEN (j.attempts + 1) >= j.max_attempts THEN $5 ELSE $6 END,
			    run_after = CASE WHEN (j.attempts + 1) >= j.max_attempts THEN j.run_after ELSE now() END,
			    locked_at = NULL,
			    locked_by = NULL,
			    last_error = $7,
			    updated_at = now(),
			    finished_at = CASE WHEN (j.attempts + 1) >= j.max_attempts THEN now() ELSE j.finished_at END
			FROM stale s
			WHERE j.id = s.id
			RETURNING j.status
		)
		SELECT status, COUNT(*)
		FROM recovered
		GROUP BY status
	`,
		StatusRunning,
		workerPool,
		olderThan.UTC(),
		limit,
		StatusDead,
		StatusPending,
		staleRunningRecoveryError,
	)
	if err != nil {
		return result, fmt.Errorf("recover stale running jobs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if scanErr := rows.Scan(&status, &count); scanErr != nil {
			return result, fmt.Errorf("scan stale recovery result: %w", scanErr)
		}
		switch status {
		case StatusPending:
			result.Recovered += count
		case StatusDead:
			result.DeadLettered += count
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return result, fmt.Errorf("read stale recovery results: %w", rowsErr)
	}
	if result.Recovered > 0 {
		notifyJobsAvailable(ctx, q.pool, workerPool)
	}
	return result, nil
}

func (q *Queue) PurgeTerminalJobs(
	ctx context.Context,
	succeededBefore time.Time,
	deadBefore time.Time,
	limit int,
) (deleted int64, err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveQueueOperation("purge_terminal_jobs", queueResultFromErr(err), time.Since(started))
	}()
	if q == nil || q.pool == nil {
		return 0, fmt.Errorf("queue is not initialized")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}
	if succeededBefore.IsZero() && deadBefore.IsZero() {
		return 0, fmt.Errorf("at least one cutoff must be provided")
	}

	if succeededBefore.IsZero() {
		succeededBefore = time.Unix(0, 0).UTC()
	}
	if deadBefore.IsZero() {
		deadBefore = time.Unix(0, 0).UTC()
	}

	// Purge by finished_at (set on terminal transition by CompleteJob /
	// FailJob / RecoverStaleRunningJobs). This intentionally diverges from
	// updated_at so that maintenance UPDATEs (e.g., later admin annotations)
	// do not extend the retention window of an already-finished job.
	//
	// Rows whose finished_at is NULL are skipped here; the migration
	// 000040_jobs_finished_at.sql backfills existing terminal rows from
	// updated_at, but anything written by an OLD worker after deploy is
	// excluded from the purge until the next terminal-transition write.
	//
	// One status per statement. An OR of succeeded and dead made the planner
	// seq-scan jobs, and the (status, finished_at) index can serve each
	// status as a bounded range.
	succeededDeleted, err := q.purgeTerminalStatus(ctx, StatusSucceeded, succeededBefore, limit)
	if err != nil {
		return 0, err
	}
	remaining := limit - int(succeededDeleted)
	if remaining <= 0 {
		return succeededDeleted, nil
	}
	deadDeleted, err := q.purgeTerminalStatus(ctx, StatusDead, deadBefore, remaining)
	if err != nil {
		return succeededDeleted, err
	}
	return succeededDeleted + deadDeleted, nil
}

func (q *Queue) purgeTerminalStatus(ctx context.Context, status string, finishedBefore time.Time, limit int) (int64, error) {
	tag, err := q.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT id
			FROM jobs
			WHERE status = $1
			  AND finished_at < $2
			ORDER BY finished_at ASC, id ASC
			LIMIT $3
		)
		DELETE FROM jobs j
		USING candidates c
		WHERE j.id = c.id
	`, status, finishedBefore.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge terminal jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}
