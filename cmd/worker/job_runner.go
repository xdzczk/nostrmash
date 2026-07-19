package main

import (
	"context"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/failure"
	"github.com/xdzczk/nostrmash/internal/traceutil"
)

type workerQueue interface {
	ClaimAvailableForPool(ctx context.Context, workerID, workerPool string, limit int) ([]jobs.Job, error)
	CompleteJob(ctx context.Context, jobID int64, workerID string) error
	FailJob(ctx context.Context, jobID int64, workerID string, errMsg string, retryDelay time.Duration) (jobs.FailureResult, error)
	RecoverStaleRunningJobs(ctx context.Context, workerPool string, olderThan time.Time, limit int) (jobs.RecoveryResult, error)
	PurgeTerminalJobs(ctx context.Context, succeededBefore, deadBefore time.Time, limit int) (int64, error)
}

func runClaimLoop(
	ctx context.Context,
	log interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	},
	queue workerQueue,
	workerID string,
	workerPool string,
	batchSize int,
	concurrency int,
	pollInterval time.Duration,
	retryDelay time.Duration,
	processJob func(jobCtx context.Context, job jobs.Job) error,
) {
	if concurrency <= 0 {
		concurrency = 1
	}
	jobQueue := make(chan jobs.Job, batchSize*concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobQueue {
				// Drain in-flight jobs even after shutdown signal.
				workerCtx := context.WithoutCancel(ctx)
				spanCtx, span := traceutil.StartSpan(workerCtx, "worker.job.execute",
					traceutil.KV("job.type", job.JobType),
				)
				started := time.Now()
				err := runJobWithRecovery(spanCtx, job, processJob)
				if err == nil {
					if completeErr := queue.CompleteJob(spanCtx, job.ID, workerID); completeErr != nil {
						class := failure.ClassifyError(completeErr)
						metrics.ObserveWorkerJobExecution(job.JobType, "complete_error", time.Since(started))
						span.End(completeErr)
						log.Error("job_complete_failed", "job_id", job.ID, "failure_class", class.Class, "failure_reason", class.Reason, "error", completeErr)
						continue
					}
					metrics.IncWorkerJob(job.JobType, "succeeded")
					metrics.ObserveWorkerJobExecution(job.JobType, "succeeded", time.Since(started))
					span.End(nil)
					log.Info("job_completed", "job_id", job.ID, "job_type", job.JobType)
					continue
				}

				failState, failErr := queue.FailJob(spanCtx, job.ID, workerID, err.Error(), retryDelay)
				if failErr != nil {
					class := failure.ClassifyError(failErr)
					metrics.ObserveWorkerJobExecution(job.JobType, "fail_mark_error", time.Since(started))
					span.End(failErr)
					log.Error("job_fail_mark_failed", "job_id", job.ID, "failure_class", class.Class, "failure_reason", class.Reason, "error", failErr)
					continue
				}
				errClass := failure.ClassifyError(err)
				if failState.Status == jobs.StatusDead {
					metrics.IncWorkerJob(job.JobType, "dead")
					metrics.ObserveWorkerJobExecution(job.JobType, "dead", time.Since(started))
					span.End(err)
					log.Error(
						"job_dead_lettered",
						"job_id", job.ID,
						"job_type", job.JobType,
						"failure_class", errClass.Class,
						"failure_reason", errClass.Reason,
						"attempts", failState.Attempts,
						"max_attempts", failState.MaxAttempts,
						"error", err,
					)
					continue
				}
				metrics.IncWorkerJob(job.JobType, "retry")
				metrics.ObserveWorkerJobExecution(job.JobType, "retry", time.Since(started))
				span.End(err)
				log.Error(
					"job_failed_retry_scheduled",
					"job_id", job.ID,
					"job_type", job.JobType,
					"failure_class", errClass.Class,
					"failure_reason", errClass.Reason,
					"attempts", failState.Attempts,
					"max_attempts", failState.MaxAttempts,
					"retry_after", retryDelay.String(),
					"error", err,
				)
			}
		}()
	}
	defer func() {
		close(jobQueue)
		wg.Wait()
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		claimCtx, claimSpan := traceutil.StartSpan(ctx, "worker.queue.claim_available")
		claimed, err := queue.ClaimAvailableForPool(claimCtx, workerID, workerPool, batchSize)
		claimSpan.End(err)
		if err != nil {
			log.Error("job_claim_failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}
		if len(claimed) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}

		for _, job := range claimed {
			select {
			case <-ctx.Done():
				return
			case jobQueue <- job:
			}
		}
	}
}

func runJobWithRecovery(ctx context.Context, job jobs.Job, processJob func(jobCtx context.Context, job jobs.Job) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = failure.FromPanic(recovered)
		}
	}()
	return processJob(ctx, job)
}
