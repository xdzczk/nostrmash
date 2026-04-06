package jobs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

func TestClaimAvailableConcurrentWorkersNoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	queue := jobs.NewQueue(pool)
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		_, err := queue.Enqueue(ctx, jobs.EnqueueParams{
			JobType:     "derive_profile",
			Payload:     []byte(`{"event_id":"abc"}`),
			RunAfter:    now,
			MaxAttempts: 3,
		})
		if err != nil {
			t.Fatalf("enqueue seed job %d: %v", i, err)
		}
	}

	type claimResult struct {
		jobs []jobs.Job
		err  error
	}
	results := make(chan claimResult, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup

	claim := func(workerID string) {
		defer wg.Done()
		<-start
		jobs, err := queue.ClaimAvailable(ctx, workerID, 2)
		results <- claimResult{jobs: jobs, err: err}
	}

	wg.Add(2)
	go claim("worker-a")
	go claim("worker-b")
	close(start)
	wg.Wait()
	close(results)

	claimedByID := make(map[int64]string)
	total := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("claim jobs: %v", result.err)
		}
		for _, job := range result.jobs {
			total++
			if prior, exists := claimedByID[job.ID]; exists {
				t.Fatalf("job %d double-claimed by %q and %q", job.ID, prior, derefString(job.LockedBy))
			}
			claimedByID[job.ID] = derefString(job.LockedBy)
		}
	}

	if total != 3 {
		t.Fatalf("expected 3 total claimed jobs, got %d", total)
	}

	remaining, err := queue.ClaimAvailable(ctx, "worker-c", 5)
	if err != nil {
		t.Fatalf("claim remaining jobs: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected 0 remaining claimable jobs, got %d", len(remaining))
	}
}

func TestFailJobRetriesThenDeadLettersAtMaxAttempts(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	queue := jobs.NewQueue(pool)
	job, err := queue.Enqueue(ctx, jobs.EnqueueParams{
		JobType:     "derive_profile",
		Payload:     []byte(`{"event_id":"abc"}`),
		MaxAttempts: 2,
		RunAfter:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	claimed, err := queue.ClaimAvailable(ctx, "worker-a", 1)
	if err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed job, got %d", len(claimed))
	}

	firstFailure, err := queue.FailJob(ctx, job.ID, "worker-a", "temporary failure", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("first failure mark: %v", err)
	}
	if firstFailure.Status != jobs.StatusPending || firstFailure.Attempts != 1 {
		t.Fatalf("unexpected first failure result: %+v", firstFailure)
	}

	immediate, err := queue.ClaimAvailable(ctx, "worker-b", 1)
	if err != nil {
		t.Fatalf("immediate claim after retry schedule: %v", err)
	}
	if len(immediate) != 0 {
		t.Fatalf("expected no immediate retry claim, got %d jobs", len(immediate))
	}

	time.Sleep(30 * time.Millisecond)

	secondClaim, err := queue.ClaimAvailable(ctx, "worker-a", 1)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(secondClaim) != 1 {
		t.Fatalf("expected retried job to become claimable, got %d jobs", len(secondClaim))
	}

	secondFailure, err := queue.FailJob(ctx, job.ID, "worker-a", "permanent failure", 0)
	if err != nil {
		t.Fatalf("second failure mark: %v", err)
	}
	if secondFailure.Status != jobs.StatusDead {
		t.Fatalf("expected dead-letter status %q, got %q", jobs.StatusDead, secondFailure.Status)
	}
	if secondFailure.Attempts != 2 {
		t.Fatalf("expected attempts=2 after dead-letter, got %d", secondFailure.Attempts)
	}

	reclaimed, err := queue.ClaimAvailable(ctx, "worker-c", 1)
	if err != nil {
		t.Fatalf("claim after dead-letter: %v", err)
	}
	if len(reclaimed) != 0 {
		t.Fatalf("expected dead-lettered job to be unclaimable, got %d jobs", len(reclaimed))
	}

	stored, err := queue.GetJobByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("load dead-lettered job: %v", err)
	}
	if stored.Status != jobs.StatusDead {
		t.Fatalf("expected stored status %q, got %q", jobs.StatusDead, stored.Status)
	}
	if stored.LastError == nil || *stored.LastError != "permanent failure" {
		t.Fatalf("expected last_error to persist permanent failure, got %v", stored.LastError)
	}
}

func TestClaimAvailableForPool_ClaimsOnlyTargetPool(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	queue := jobs.NewQueue(pool)
	_, err := queue.Enqueue(ctx, jobs.EnqueueParams{
		JobType:  jobs.JobTypeDeriveEventBundle,
		Payload:  []byte(`{"event_id":"a"}`),
		RunAfter: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("enqueue default job: %v", err)
	}
	_, err = queue.Enqueue(ctx, jobs.EnqueueParams{
		JobType:  jobs.JobTypeTrustComputeGlobalScore,
		Payload:  []byte(`{"run_id":1}`),
		RunAfter: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("enqueue trust job: %v", err)
	}

	defaultJobs, err := queue.ClaimAvailableForPool(ctx, "worker-default", jobs.WorkerPoolDefault, 10)
	if err != nil {
		t.Fatalf("claim default pool: %v", err)
	}
	if len(defaultJobs) != 1 {
		t.Fatalf("expected one default job, got %d", len(defaultJobs))
	}
	if defaultJobs[0].WorkerPool != jobs.WorkerPoolDefault {
		t.Fatalf("expected default worker pool, got %q", defaultJobs[0].WorkerPool)
	}

	trustJobs, err := queue.ClaimAvailableForPool(ctx, "worker-trust", jobs.WorkerPoolTrust, 10)
	if err != nil {
		t.Fatalf("claim trust pool: %v", err)
	}
	if len(trustJobs) != 1 {
		t.Fatalf("expected one trust job, got %d", len(trustJobs))
	}
	if trustJobs[0].WorkerPool != jobs.WorkerPoolTrust {
		t.Fatalf("expected trust worker pool, got %q", trustJobs[0].WorkerPool)
	}
}

func setupSchemaPool(t *testing.T, ctx context.Context, dbURL string) *pgxpool.Pool {
	t.Helper()
	return dbtest.SetupSchemaPool(t, ctx, dbURL, "jobs")
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	return dbtest.DatabaseURL(t, "jobs")
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
