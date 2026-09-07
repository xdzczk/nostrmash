package apptrustworker

import (
	"context"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/failure"
)

type workerQueue interface {
	ClaimAvailableForPool(ctx context.Context, workerID, workerPool string, limit int) ([]jobs.Job, error)
	CompleteJob(ctx context.Context, jobID int64, workerID string) error
	FailJob(ctx context.Context, jobID int64, workerID string, errMsg string, retryDelay time.Duration) (jobs.FailureResult, error)
	RecoverStaleRunningJobs(ctx context.Context, workerPool string, olderThan time.Time, limit int) (jobs.RecoveryResult, error)
	WaitForWork(ctx context.Context, max time.Duration)
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
				workerCtx := context.WithoutCancel(ctx)
				started := time.Now()
				err := runJobWithRecovery(workerCtx, job, processJob)
				if err == nil {
					if completeErr := queue.CompleteJob(workerCtx, job.ID, workerID); completeErr != nil {
						log.Error("job_complete_failed", "job_id", job.ID, "error", completeErr)
						continue
					}
					metrics.IncWorkerJob(job.JobType, "succeeded")
					metrics.ObserveWorkerJobExecution(job.JobType, "succeeded", time.Since(started))
					continue
				}
				// Scale the retry delay with how often this job has already
				// failed, so persistent failures back off instead of
				// hammering the queue in lockstep every fixed interval.
				backoff := jobs.RetryDelay(job.Attempts, retryDelay, jobs.DefaultMaxRetryDelay)
				failState, failErr := queue.FailJob(workerCtx, job.ID, workerID, err.Error(), backoff)
				if failErr != nil {
					log.Error("job_fail_mark_failed", "job_id", job.ID, "error", failErr)
					continue
				}
				if failState.Status == jobs.StatusDead {
					metrics.IncWorkerJob(job.JobType, "dead")
					metrics.ObserveWorkerJobExecution(job.JobType, "dead", time.Since(started))
					log.Error("job_dead_lettered", "job_id", job.ID, "job_type", job.JobType, "error", err)
					continue
				}
				metrics.IncWorkerJob(job.JobType, "retry")
				metrics.ObserveWorkerJobExecution(job.JobType, "retry", time.Since(started))
				log.Error("job_failed_retry_scheduled", "job_id", job.ID, "job_type", job.JobType, "error", err)
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
		claimed, err := queue.ClaimAvailableForPool(ctx, workerID, workerPool, batchSize)
		if err != nil {
			log.Error("job_claim_failed", "error", err)
			queue.WaitForWork(ctx, pollInterval)
			continue
		}
		if len(claimed) == 0 {
			// Wake on LISTEN/NOTIFY (or the poll-interval safety net) instead
			// of always sleeping a full second after an empty claim.
			queue.WaitForWork(ctx, pollInterval)
			continue
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
