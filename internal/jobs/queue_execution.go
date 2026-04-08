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

func (q *Queue) CompleteJob(ctx context.Context, jobID int64, workerID string) (err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveQueueOperation("complete_job", queueResultFromErr(err), time.Since(started))
	}()
	if q == nil || q.pool == nil {
		return fmt.Errorf("queue is not initialized")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return fmt.Errorf("worker id is required")
	}

	tag, err := q.pool.Exec(ctx, `
		UPDATE jobs
		SET status = $1,
		    locked_at = NULL,
		    locked_by = NULL,
		    last_error = NULL,
		    updated_at = now()
		WHERE id = $2
		  AND status = $3
		  AND locked_by = $4
	`,
		StatusSucceeded,
		jobID,
		StatusRunning,
		workerID,
	)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotOwned
	}
	return nil
}

func (q *Queue) FailJob(
	ctx context.Context,
	jobID int64,
	workerID string,
	lastError string,
	retryAfter time.Duration,
) (result FailureResult, err error) {
	result = FailureResult{}
	started := time.Now()
	defer func() {
		metrics.ObserveQueueOperation("fail_job", queueResultFromErr(err), time.Since(started))
	}()
	if q == nil || q.pool == nil {
		return result, fmt.Errorf("queue is not initialized")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return result, fmt.Errorf("worker id is required")
	}
	lastError = strings.TrimSpace(lastError)
	if lastError == "" {
		lastError = "unknown job error"
	}
	if retryAfter < 0 {
		retryAfter = 0
	}

	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var attempts int
	var maxAttempts int
	err = tx.QueryRow(ctx, `
		SELECT attempts, max_attempts
		FROM jobs
		WHERE id = $1
		  AND status = $2
		  AND locked_by = $3
		FOR UPDATE
	`,
		jobID,
		StatusRunning,
		workerID,
	).Scan(&attempts, &maxAttempts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return result, ErrJobNotOwned
		}
		return result, fmt.Errorf("load job failure state: %w", err)
	}

	nextAttempts := attempts + 1
	nextStatus := StatusPending
	nextRunAfter := time.Now().UTC().Add(retryAfter)
	if nextAttempts >= maxAttempts {
		nextStatus = StatusDead
		nextRunAfter = time.Now().UTC()
	}

	_, err = tx.Exec(ctx, `
		UPDATE jobs
		SET status = $1,
		    attempts = $2,
		    run_after = $3,
		    locked_at = NULL,
		    locked_by = NULL,
		    last_error = $4,
		    updated_at = now()
		WHERE id = $5
	`,
		nextStatus,
		nextAttempts,
		nextRunAfter,
		lastError,
		jobID,
	)
	if err != nil {
		return result, fmt.Errorf("mark job failure: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit failure tx: %w", err)
	}

	return FailureResult{
		Status:      nextStatus,
		Attempts:    nextAttempts,
		MaxAttempts: maxAttempts,
	}, nil
}
