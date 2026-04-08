package main

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/jobs"
)

type fakeTrustWorkerLogger struct{}

func (fakeTrustWorkerLogger) Info(string, ...any)  {}
func (fakeTrustWorkerLogger) Error(string, ...any) {}

type fakeTrustWorkerQueue struct {
	mu           sync.Mutex
	claimBatches [][]jobs.Job
	claimCalls   int
	completedIDs []int64
	failedIDs    []int64
	recoverCalls int
	recoverPools []string
}

func (f *fakeTrustWorkerQueue) ClaimAvailableForPool(ctx context.Context, _ string, _ string, _ int) ([]jobs.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimCalls < len(f.claimBatches) {
		out := f.claimBatches[f.claimCalls]
		f.claimCalls++
		return out, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return []jobs.Job{}, nil
	}
}

func (f *fakeTrustWorkerQueue) CompleteJob(_ context.Context, jobID int64, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completedIDs = append(f.completedIDs, jobID)
	return nil
}

func (f *fakeTrustWorkerQueue) FailJob(_ context.Context, jobID int64, _ string, _ string, _ time.Duration) (jobs.FailureResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failedIDs = append(f.failedIDs, jobID)
	return jobs.FailureResult{Status: jobs.StatusPending, Attempts: 1, MaxAttempts: 5}, nil
}

func (f *fakeTrustWorkerQueue) RecoverStaleRunningJobs(_ context.Context, workerPool string, _ time.Time, _ int) (jobs.RecoveryResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recoverCalls++
	f.recoverPools = append(f.recoverPools, workerPool)
	return jobs.RecoveryResult{}, nil
}

func TestRunClaimLoop_ProcessesTrustJobsConcurrently(t *testing.T) {
	queue := &fakeTrustWorkerQueue{
		claimBatches: [][]jobs.Job{
			{
				{ID: 1, JobType: jobs.JobTypeTrustComputeGlobalScore, Payload: json.RawMessage(`{"run_id":1}`)},
				{ID: 2, JobType: jobs.JobTypeTrustComputeGlobalScore, Payload: json.RawMessage(`{"run_id":2}`)},
				{ID: 3, JobType: jobs.JobTypeTrustComputeGlobalScore, Payload: json.RawMessage(`{"run_id":3}`)},
				{ID: 4, JobType: jobs.JobTypeTrustComputeGlobalScore, Payload: json.RawMessage(`{"run_id":4}`)},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	var active int64
	var maxActive int64
	runClaimLoop(
		ctx,
		fakeTrustWorkerLogger{},
		queue,
		"trust-worker-a",
		jobs.WorkerPoolTrust,
		4,
		2,
		5*time.Millisecond,
		5*time.Millisecond,
		func(_ context.Context, job jobs.Job) error {
			_ = job
			current := atomic.AddInt64(&active, 1)
			for {
				prev := atomic.LoadInt64(&maxActive)
				if current <= prev || atomic.CompareAndSwapInt64(&maxActive, prev, current) {
					break
				}
			}
			time.Sleep(40 * time.Millisecond)
			atomic.AddInt64(&active, -1)
			return nil
		},
	)

	if atomic.LoadInt64(&maxActive) < 2 {
		t.Fatalf("expected concurrent processing, max active=%d", maxActive)
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.completedIDs) != 4 {
		t.Fatalf("expected 4 completed jobs, got %d", len(queue.completedIDs))
	}
}

func TestRunClaimLoop_DrainsQueuedTrustJobsAfterShutdownSignal(t *testing.T) {
	queue := &fakeTrustWorkerQueue{
		claimBatches: [][]jobs.Job{
			{
				{ID: 10, JobType: jobs.JobTypeTrustSyncGraphRedis, Payload: json.RawMessage(`{"run_id":10}`)},
				{ID: 11, JobType: jobs.JobTypeTrustComputeGlobalScore, Payload: json.RawMessage(`{"run_id":11}`)},
				{ID: 12, JobType: jobs.JobTypeTrustPromoteRun, Payload: json.RawMessage(`{"run_id":12}`)},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	runClaimLoop(
		ctx,
		fakeTrustWorkerLogger{},
		queue,
		"trust-worker-a",
		jobs.WorkerPoolTrust,
		3,
		1,
		5*time.Millisecond,
		5*time.Millisecond,
		func(_ context.Context, job jobs.Job) error {
			_ = job
			time.Sleep(15 * time.Millisecond)
			return nil
		},
	)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.completedIDs) != 3 {
		t.Fatalf("expected queued jobs to drain after shutdown, got %d completed", len(queue.completedIDs))
	}
}

func TestRunClaimLoop_RecoversFromPanicAndFailsTrustJob(t *testing.T) {
	queue := &fakeTrustWorkerQueue{
		claimBatches: [][]jobs.Job{
			{
				{ID: 99, JobType: jobs.JobTypeTrustComputeGlobalScore, Payload: json.RawMessage(`{"run_id":99}`)},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	runClaimLoop(
		ctx,
		fakeTrustWorkerLogger{},
		queue,
		"trust-worker-a",
		jobs.WorkerPoolTrust,
		1,
		1,
		5*time.Millisecond,
		5*time.Millisecond,
		func(_ context.Context, _ jobs.Job) error {
			panic("job panic")
		},
	)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.failedIDs) != 1 || queue.failedIDs[0] != 99 {
		t.Fatalf("expected panicing job to be failed, failed ids=%v", queue.failedIDs)
	}
}

func TestResolveBuildVersion(t *testing.T) {
	orig := buildVersion
	t.Cleanup(func() {
		buildVersion = orig
	})

	buildVersion = "binary-v1.2.3"
	if got := resolveBuildVersion("env-v9.9.9"); got != "binary-v1.2.3" {
		t.Fatalf("expected build version override, got %q", got)
	}

	buildVersion = ""
	if got := resolveBuildVersion(" env-v9.9.9 "); got != "env-v9.9.9" {
		t.Fatalf("expected fallback to app version, got %q", got)
	}
}

func TestRunStaleRecoveryLoop_InvokesQueueRecoveryPeriodically(t *testing.T) {
	queue := &fakeTrustWorkerQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go runStaleRecoveryLoop(
		ctx,
		fakeTrustWorkerLogger{},
		queue,
		jobs.WorkerPoolTrust,
		config.WorkerJobRecoveryConfig{
			RunningTimeout:          10 * time.Millisecond,
			StaleRecoveryInterval:   10 * time.Millisecond,
			StaleRecoveryBatchLimit: 5,
		},
	)

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		queue.mu.Lock()
		calls := queue.recoverCalls
		pools := append([]string(nil), queue.recoverPools...)
		queue.mu.Unlock()
		if calls >= 2 {
			for _, pool := range pools {
				if pool != jobs.WorkerPoolTrust {
					t.Fatalf("expected stale recovery pool %q, got %q", jobs.WorkerPoolTrust, pool)
				}
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected stale recovery loop to invoke queue recovery at least twice")
}

func TestTrustWorkerModeLabel(t *testing.T) {
	tests := []struct {
		name               string
		enableRedisSync    bool
		enableScoreCompute bool
		want               string
	}{
		{
			name:               "redis sync and compute",
			enableRedisSync:    true,
			enableScoreCompute: true,
			want:               "redis-sync+compute",
		},
		{
			name:               "redis sync only",
			enableRedisSync:    true,
			enableScoreCompute: false,
			want:               "redis-sync-only",
		},
		{
			name:               "compute only",
			enableRedisSync:    false,
			enableScoreCompute: true,
			want:               "postgres-only-compute",
		},
		{
			name:               "invalid none enabled",
			enableRedisSync:    false,
			enableScoreCompute: false,
			want:               "invalid",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := trustWorkerModeLabel(tc.enableRedisSync, tc.enableScoreCompute)
			if got != tc.want {
				t.Fatalf("expected mode %q, got %q", tc.want, got)
			}
		})
	}
}
