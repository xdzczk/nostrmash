package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("worker")

	cfg, err := config.Load("worker")
	if err != nil {
		log.Error("config", "error", err)
		os.Exit(1)
	}

	pool, err := store.OpenPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("db_connect", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	appVersion := os.Getenv("APP_VERSION")
	if appVersion == "" {
		appVersion = "dev"
	}
	if err := store.Migrate(ctx, pool, appVersion); err != nil {
		log.Error("migrate", "error", err)
		os.Exit(1)
	}

	queue := jobs.NewQueue(pool)
	handlers := derivation.NewHandlers(pool)
	workerID := resolveWorkerID()
	runMetricsEndpoint(ctx, log, cfg.MetricsAddr)
	const (
		claimBatchSize = 10
		pollInterval   = 1 * time.Second
		retryDelay     = 5 * time.Second
	)

	log.Info("worker_started", "worker_id", workerID, "claim_batch_size", claimBatchSize)
	runClaimLoop(
		ctx,
		log,
		queue,
		workerID,
		claimBatchSize,
		cfg.WorkerConcurrency,
		pollInterval,
		retryDelay,
		func(job jobs.Job) error {
			return derivation.ProcessJob(ctx, handlers, derivation.Job{
				JobType: job.JobType,
				Payload: job.Payload,
			})
		},
	)

	<-ctx.Done()
	log.Info("shutdown_complete")
}

type workerQueue interface {
	ClaimAvailable(ctx context.Context, workerID string, limit int) ([]jobs.Job, error)
	CompleteJob(ctx context.Context, jobID int64, workerID string) error
	FailJob(ctx context.Context, jobID int64, workerID string, errMsg string, retryDelay time.Duration) (jobs.FailureResult, error)
}

func runClaimLoop(
	ctx context.Context,
	log interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	},
	queue workerQueue,
	workerID string,
	batchSize int,
	concurrency int,
	pollInterval time.Duration,
	retryDelay time.Duration,
	processJob func(job jobs.Job) error,
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
				err := processJob(job)
				if err == nil {
					if completeErr := queue.CompleteJob(workerCtx, job.ID, workerID); completeErr != nil {
						log.Error("job_complete_failed", "job_id", job.ID, "error", completeErr)
						continue
					}
					metrics.IncWorkerJob(job.JobType, "succeeded")
					log.Info("job_completed", "job_id", job.ID, "job_type", job.JobType)
					continue
				}

				failure, failErr := queue.FailJob(workerCtx, job.ID, workerID, err.Error(), retryDelay)
				if failErr != nil {
					log.Error("job_fail_mark_failed", "job_id", job.ID, "error", failErr)
					continue
				}
				if failure.Status == jobs.StatusDead {
					metrics.IncWorkerJob(job.JobType, "dead")
					log.Error(
						"job_dead_lettered",
						"job_id", job.ID,
						"job_type", job.JobType,
						"attempts", failure.Attempts,
						"max_attempts", failure.MaxAttempts,
						"error", err,
					)
					continue
				}
				metrics.IncWorkerJob(job.JobType, "retry")
				log.Error(
					"job_failed_retry_scheduled",
					"job_id", job.ID,
					"job_type", job.JobType,
					"attempts", failure.Attempts,
					"max_attempts", failure.MaxAttempts,
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

		claimed, err := queue.ClaimAvailable(ctx, workerID, batchSize)
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

func runMetricsEndpoint(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}, addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Info("metrics_listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics_server", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}

func resolveWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	host = strings.TrimSpace(host)
	if host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}
