package main

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
)

type fakeWorkerLogger struct{}

func (fakeWorkerLogger) Info(string, ...any)  {}
func (fakeWorkerLogger) Error(string, ...any) {}

type fakeWorkerQueue struct {
	mu           sync.Mutex
	claimBatches [][]jobs.Job
	claimCalls   int
	completedIDs []int64
	failedIDs    []int64
}

func (f *fakeWorkerQueue) ClaimAvailableForPool(ctx context.Context, _ string, _ string, _ int) ([]jobs.Job, error) {
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

func (f *fakeWorkerQueue) CompleteJob(_ context.Context, jobID int64, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completedIDs = append(f.completedIDs, jobID)
	return nil
}

func (f *fakeWorkerQueue) FailJob(_ context.Context, jobID int64, _ string, _ string, _ time.Duration) (jobs.FailureResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failedIDs = append(f.failedIDs, jobID)
	return jobs.FailureResult{Status: jobs.StatusPending, Attempts: 1, MaxAttempts: 5}, nil
}

func (f *fakeWorkerQueue) PurgeTerminalJobs(_ context.Context, _ time.Time, _ time.Time, _ int) (int64, error) {
	return 0, nil
}

func TestRunClaimLoop_ProcessesJobsConcurrently(t *testing.T) {
	queue := &fakeWorkerQueue{
		claimBatches: [][]jobs.Job{
			{
				{ID: 1, JobType: "derive", Payload: json.RawMessage(`{}`)},
				{ID: 2, JobType: "derive", Payload: json.RawMessage(`{}`)},
				{ID: 3, JobType: "derive", Payload: json.RawMessage(`{}`)},
				{ID: 4, JobType: "derive", Payload: json.RawMessage(`{}`)},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	var active int64
	var maxActive int64
	runClaimLoop(
		ctx,
		fakeWorkerLogger{},
		queue,
		"worker-a",
		jobs.WorkerPoolDefault,
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

func TestRunClaimLoop_DrainsQueuedJobsAfterShutdownSignal(t *testing.T) {
	queue := &fakeWorkerQueue{
		claimBatches: [][]jobs.Job{
			{
				{ID: 10, JobType: "derive", Payload: json.RawMessage(`{}`)},
				{ID: 11, JobType: "derive", Payload: json.RawMessage(`{}`)},
				{ID: 12, JobType: "derive", Payload: json.RawMessage(`{}`)},
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
		fakeWorkerLogger{},
		queue,
		"worker-a",
		jobs.WorkerPoolDefault,
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

func TestRunClaimLoop_RecoversFromPanicAndFailsJob(t *testing.T) {
	queue := &fakeWorkerQueue{
		claimBatches: [][]jobs.Job{
			{
				{ID: 99, JobType: "derive", Payload: json.RawMessage(`{}`)},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	runClaimLoop(
		ctx,
		fakeWorkerLogger{},
		queue,
		"worker-a",
		jobs.WorkerPoolDefault,
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
