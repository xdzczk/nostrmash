package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusDead      = "dead"
)

var (
	ErrNotFound    = errors.New("job not found")
	ErrJobNotOwned = errors.New("job is not locked by worker")
)

type Queue struct {
	pool *pgxpool.Pool
}

type Job struct {
	ID             int64
	JobType        string
	WorkerPool     string
	Payload        json.RawMessage
	IdempotencyKey *string
	Status         string
	Attempts       int
	MaxAttempts    int
	RunAfter       time.Time
	LockedAt       *time.Time
	LockedBy       *string
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type EnqueueParams struct {
	JobType        string
	WorkerPool     string
	Payload        json.RawMessage
	IdempotencyKey string
	MaxAttempts    int
	RunAfter       time.Time
}

type FailureResult struct {
	Status      string
	Attempts    int
	MaxAttempts int
}

func NewQueue(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

func (q *Queue) Enqueue(ctx context.Context, params EnqueueParams) (job *Job, err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveQueueOperation("enqueue", queueResultFromErr(err), time.Since(started))
	}()
	if q == nil || q.pool == nil {
		return nil, fmt.Errorf("queue is not initialized")
	}
	jobType := strings.TrimSpace(params.JobType)
	if jobType == "" {
		return nil, fmt.Errorf("job type is required")
	}

	payload := params.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}

	maxAttempts := params.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}

	runAfter := params.RunAfter.UTC()
	if runAfter.IsZero() {
		runAfter = time.Now().UTC()
	}

	key := strings.TrimSpace(params.IdempotencyKey)
	var idempotencyKey *string
	if key != "" {
		idempotencyKey = &key
	}

	workerPool := strings.TrimSpace(params.WorkerPool)
	if workerPool == "" {
		workerPool = WorkerPoolForJobType(jobType)
	}

	row := q.pool.QueryRow(ctx, `
		INSERT INTO jobs (job_type, worker_pool, payload, idempotency_key, max_attempts, run_after)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET updated_at = jobs.updated_at
		RETURNING id, job_type, worker_pool, payload, idempotency_key, status, attempts, max_attempts,
		          run_after, locked_at, locked_by, last_error, created_at, updated_at
	`,
		jobType,
		workerPool,
		payload,
		idempotencyKey,
		maxAttempts,
		runAfter,
	)

	job, err = scanJobRow(row)
	if err != nil {
		return nil, fmt.Errorf("enqueue job: %w", err)
	}
	return job, nil
}

func (q *Queue) ClaimAvailable(ctx context.Context, workerID string, limit int) (out []Job, err error) {
	return q.ClaimAvailableForPool(ctx, workerID, WorkerPoolDefault, limit)
}

func (q *Queue) ClaimAvailableForPool(ctx context.Context, workerID string, workerPool string, limit int) (out []Job, err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveQueueOperation("claim_available", queueResultFromErr(err), time.Since(started))
	}()
	if q == nil || q.pool == nil {
		return nil, fmt.Errorf("queue is not initialized")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return nil, fmt.Errorf("worker id is required")
	}
	workerPool = strings.TrimSpace(workerPool)
	if workerPool == "" {
		return nil, fmt.Errorf("worker pool is required")
	}
	if limit <= 0 {
		limit = 1
	}

	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		WITH claimable AS (
			SELECT id
			FROM jobs
			WHERE status = $1
			  AND worker_pool = $2
			  AND run_after <= now()
			ORDER BY run_after ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		),
		claimed AS (
			UPDATE jobs j
			SET status = $4,
			    locked_at = now(),
			    locked_by = $5,
			    updated_at = now()
			FROM claimable c
			WHERE j.id = c.id
			RETURNING j.id, j.job_type, j.worker_pool, j.payload, j.idempotency_key, j.status, j.attempts, j.max_attempts,
			          j.run_after, j.locked_at, j.locked_by, j.last_error, j.created_at, j.updated_at
		)
		SELECT id, job_type, worker_pool, payload, idempotency_key, status, attempts, max_attempts,
		       run_after, locked_at, locked_by, last_error, created_at, updated_at
		FROM claimed
		ORDER BY run_after ASC, id ASC
	`,
		StatusPending,
		workerPool,
		limit,
		StatusRunning,
		workerID,
	)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	defer rows.Close()

	out = make([]Job, 0, limit)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan claimed job: %w", err)
		}
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read claimed jobs: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim tx: %w", err)
	}
	return out, nil
}

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

func (q *Queue) GetJobByID(ctx context.Context, jobID int64) (*Job, error) {
	if q == nil || q.pool == nil {
		return nil, fmt.Errorf("queue is not initialized")
	}

	row := q.pool.QueryRow(ctx, `
		SELECT id, job_type, worker_pool, payload, idempotency_key, status, attempts, max_attempts,
		       run_after, locked_at, locked_by, last_error, created_at, updated_at
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

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJobRow(row rowScanner) (*Job, error) {
	job, err := scanJob(row)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func scanJob(row rowScanner) (Job, error) {
	var job Job
	var payloadBytes []byte
	err := row.Scan(
		&job.ID,
		&job.JobType,
		&job.WorkerPool,
		&payloadBytes,
		&job.IdempotencyKey,
		&job.Status,
		&job.Attempts,
		&job.MaxAttempts,
		&job.RunAfter,
		&job.LockedAt,
		&job.LockedBy,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		return Job{}, err
	}
	job.Payload = append(json.RawMessage(nil), payloadBytes...)
	return job, nil
}

func queueResultFromErr(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, ErrJobNotOwned) {
		return "not_owned"
	}
	if errors.Is(err, ErrNotFound) {
		return "not_found"
	}
	return "error"
}
