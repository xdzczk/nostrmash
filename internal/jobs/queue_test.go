package jobs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestClaimAvailableConcurrentWorkersNoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	queue := jobs.NewQueue(pool)
	now := time.Now().UTC().Add(-1 * time.Second)
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
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	queue := jobs.NewQueue(pool)
	job, err := queue.Enqueue(ctx, jobs.EnqueueParams{
		JobType:     "derive_profile",
		Payload:     []byte(`{"event_id":"abc"}`),
		MaxAttempts: 2,
		RunAfter:    time.Now().UTC().Add(-1 * time.Second),
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
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	queue := jobs.NewQueue(pool)
	readyAt := time.Now().UTC().Add(-1 * time.Second)
	_, err := queue.Enqueue(ctx, jobs.EnqueueParams{
		JobType:  jobs.JobTypeDeriveEventBundle,
		Payload:  []byte(`{"event_id":"a"}`),
		RunAfter: readyAt,
	})
	if err != nil {
		t.Fatalf("enqueue default job: %v", err)
	}
	_, err = queue.Enqueue(ctx, jobs.EnqueueParams{
		JobType:  jobs.JobTypeTrustComputeGlobalScore,
		Payload:  []byte(`{"run_id":1}`),
		RunAfter: readyAt,
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

func TestPurgeTerminalJobs_DeletesOnlyOldTerminalRows(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)

	oldSucceeded := insertTerminalJobForTest(t, ctx, pool, jobs.StatusSucceeded, time.Now().UTC().Add(-40*24*time.Hour))
	oldDead := insertTerminalJobForTest(t, ctx, pool, jobs.StatusDead, time.Now().UTC().Add(-220*24*time.Hour))
	_ = insertTerminalJobForTest(t, ctx, pool, jobs.StatusSucceeded, time.Now().UTC().Add(-5*24*time.Hour))
	_ = insertTerminalJobForTest(t, ctx, pool, jobs.StatusPending, time.Now().UTC().Add(-60*24*time.Hour))

	deleted, err := queue.PurgeTerminalJobs(
		ctx,
		time.Now().UTC().Add(-30*24*time.Hour),
		time.Now().UTC().Add(-180*24*time.Hour),
		100,
	)
	if err != nil {
		t.Fatalf("purge terminal jobs: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected two terminal jobs to be deleted, got %d", deleted)
	}

	assertJobMissing(t, ctx, pool, oldSucceeded)
	assertJobMissing(t, ctx, pool, oldDead)

	var remaining int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&remaining); err != nil {
		t.Fatalf("count remaining jobs: %v", err)
	}
	if remaining != 2 {
		t.Fatalf("expected two remaining jobs, got %d", remaining)
	}
}

// TestPurgeTerminalJobs_PurgesByFinishedAtNotUpdatedAt is the regression
// guard for the migration to finished_at-based retention. A succeeded row
// whose finished_at is old must be deleted even if updated_at is recent (e.g.
// a maintenance UPDATE touched it). Conversely, a row whose finished_at is
// recent must survive even if updated_at is ancient.
func TestPurgeTerminalJobs_PurgesByFinishedAtNotUpdatedAt(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)

	now := time.Now().UTC()
	// Old finished_at, recent updated_at: should be purged.
	oldFinishedRecentTouched := insertTerminalJobWithFinishedAtForTest(
		t, ctx, pool,
		jobs.StatusSucceeded,
		now.Add(-40*24*time.Hour), // updated_at recent enough vs cutoff
		ptrTime(now.Add(-40*24*time.Hour)),
	)
	// Recent finished_at, ancient updated_at: should survive.
	recentFinishedAncientTouched := insertTerminalJobWithFinishedAtForTest(
		t, ctx, pool,
		jobs.StatusSucceeded,
		now.Add(-365*24*time.Hour),
		ptrTime(now.Add(-1*time.Hour)),
	)
	// Terminal but no finished_at (legacy row written by an OLD worker after
	// the migration but before this code is rolled out): must NOT be purged.
	terminalNullFinished := insertTerminalJobWithFinishedAtForTest(
		t, ctx, pool,
		jobs.StatusDead,
		now.Add(-365*24*time.Hour),
		nil,
	)

	deleted, err := queue.PurgeTerminalJobs(
		ctx,
		now.Add(-24*time.Hour),
		now.Add(-14*24*time.Hour),
		100,
	)
	if err != nil {
		t.Fatalf("purge terminal jobs: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected exactly one purge (old finished_at), got %d", deleted)
	}
	assertJobMissing(t, ctx, pool, oldFinishedRecentTouched)
	assertJobPresent(t, ctx, pool, recentFinishedAncientTouched)
	assertJobPresent(t, ctx, pool, terminalNullFinished)
}

// TestCompleteJob_SetsFinishedAt and TestFailJob_SetsFinishedAtOnlyOnDead
// pin the finished_at write semantics that retention depends on.
func TestCompleteJob_SetsFinishedAt(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)
	job, err := queue.Enqueue(ctx, jobs.EnqueueParams{
		JobType:     "derive_profile",
		Payload:     []byte(`{"event_id":"abc"}`),
		MaxAttempts: 3,
		RunAfter:    time.Now().UTC().Add(-time.Second),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := queue.ClaimAvailable(ctx, "worker-a", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(claimed))
	}
	if claimed[0].FinishedAt != nil {
		t.Fatalf("expected freshly claimed job to have nil finished_at, got %v", claimed[0].FinishedAt)
	}
	if err := queue.CompleteJob(ctx, job.ID, "worker-a"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	stored, err := queue.GetJobByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.Status != jobs.StatusSucceeded {
		t.Fatalf("expected succeeded, got %q", stored.Status)
	}
	if stored.FinishedAt == nil {
		t.Fatalf("expected finished_at to be set after CompleteJob")
	}
	if time.Since(*stored.FinishedAt) > time.Minute {
		t.Fatalf("expected finished_at to be recent, got %s", stored.FinishedAt)
	}
}

func TestFailJob_SetsFinishedAtOnlyOnDead(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)
	job, err := queue.Enqueue(ctx, jobs.EnqueueParams{
		JobType:     "derive_profile",
		Payload:     []byte(`{"event_id":"abc"}`),
		MaxAttempts: 2,
		RunAfter:    time.Now().UTC().Add(-time.Second),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := queue.ClaimAvailable(ctx, "worker-a", 1); err != nil {
		t.Fatalf("claim: %v", err)
	}
	first, err := queue.FailJob(ctx, job.ID, "worker-a", "transient", time.Millisecond)
	if err != nil {
		t.Fatalf("first fail: %v", err)
	}
	if first.Status != jobs.StatusPending {
		t.Fatalf("expected first fail to retry, got %q", first.Status)
	}
	stored, err := queue.GetJobByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("get after retry: %v", err)
	}
	if stored.FinishedAt != nil {
		t.Fatalf("expected finished_at to remain nil after retry-to-pending, got %v", stored.FinishedAt)
	}

	time.Sleep(10 * time.Millisecond)
	if _, err := queue.ClaimAvailable(ctx, "worker-a", 1); err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	second, err := queue.FailJob(ctx, job.ID, "worker-a", "permanent", 0)
	if err != nil {
		t.Fatalf("second fail: %v", err)
	}
	if second.Status != jobs.StatusDead {
		t.Fatalf("expected dead after exhausting attempts, got %q", second.Status)
	}
	stored, err = queue.GetJobByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("get after dead: %v", err)
	}
	if stored.FinishedAt == nil {
		t.Fatalf("expected finished_at to be set after FailJob -> dead")
	}
	if time.Since(*stored.FinishedAt) > time.Minute {
		t.Fatalf("expected finished_at to be recent, got %s", stored.FinishedAt)
	}
}

func TestRecoverStaleRunningJobs_RequeuesStaleRunningJob(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)
	job := insertRunningJobForRecoveryTest(
		t,
		ctx,
		pool,
		jobs.WorkerPoolDefault,
		1,
		3,
		time.Now().UTC().Add(-2*time.Minute),
		"lost-worker-a",
	)

	result, err := queue.RecoverStaleRunningJobs(ctx, jobs.WorkerPoolDefault, time.Now().UTC().Add(-30*time.Second), 10)
	if err != nil {
		t.Fatalf("recover stale jobs: %v", err)
	}
	if result.Recovered != 1 || result.DeadLettered != 0 {
		t.Fatalf("unexpected recovery result: %+v", result)
	}

	stored, err := queue.GetJobByID(ctx, job)
	if err != nil {
		t.Fatalf("load recovered job: %v", err)
	}
	if stored.Status != jobs.StatusPending {
		t.Fatalf("expected recovered job to become pending, got %q", stored.Status)
	}
	if stored.Attempts != 2 {
		t.Fatalf("expected attempts incremented to 2, got %d", stored.Attempts)
	}
	if stored.LockedAt != nil || stored.LockedBy != nil {
		t.Fatalf("expected lock fields to be cleared, locked_at=%v locked_by=%v", stored.LockedAt, stored.LockedBy)
	}
	if stored.LastError == nil || *stored.LastError == "" {
		t.Fatalf("expected recovery last_error to be set")
	}

	claimed, err := queue.ClaimAvailable(ctx, "worker-a", 1)
	if err != nil {
		t.Fatalf("claim recovered job: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != job {
		t.Fatalf("expected recovered job to be claimable, got %+v", claimed)
	}
}

func TestRecoverStaleRunningJobs_DeadLettersWhenAttemptsExhausted(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)
	job := insertRunningJobForRecoveryTest(
		t,
		ctx,
		pool,
		jobs.WorkerPoolDefault,
		2,
		3,
		time.Now().UTC().Add(-3*time.Minute),
		"lost-worker-b",
	)

	result, err := queue.RecoverStaleRunningJobs(ctx, jobs.WorkerPoolDefault, time.Now().UTC().Add(-1*time.Minute), 10)
	if err != nil {
		t.Fatalf("recover stale jobs: %v", err)
	}
	if result.Recovered != 0 || result.DeadLettered != 1 {
		t.Fatalf("unexpected recovery result: %+v", result)
	}

	stored, err := queue.GetJobByID(ctx, job)
	if err != nil {
		t.Fatalf("load dead-lettered recovered job: %v", err)
	}
	if stored.Status != jobs.StatusDead {
		t.Fatalf("expected stale exhausted job to be dead, got %q", stored.Status)
	}
	if stored.Attempts != 3 {
		t.Fatalf("expected attempts incremented to max=3, got %d", stored.Attempts)
	}
	if stored.LockedAt != nil || stored.LockedBy != nil {
		t.Fatalf("expected lock fields to be cleared, locked_at=%v locked_by=%v", stored.LockedAt, stored.LockedBy)
	}

	claimed, err := queue.ClaimAvailable(ctx, "worker-a", 1)
	if err != nil {
		t.Fatalf("claim dead-lettered recovered job: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected dead-lettered recovered job to be unclaimable, got %d", len(claimed))
	}
}

func TestRecoverStaleRunningJobs_DoesNotTouchFreshRunningJob(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)
	job := insertRunningJobForRecoveryTest(
		t,
		ctx,
		pool,
		jobs.WorkerPoolDefault,
		0,
		5,
		time.Now().UTC().Add(-3*time.Second),
		"active-worker",
	)

	result, err := queue.RecoverStaleRunningJobs(ctx, jobs.WorkerPoolDefault, time.Now().UTC().Add(-10*time.Second), 10)
	if err != nil {
		t.Fatalf("recover stale jobs: %v", err)
	}
	if result.Recovered != 0 || result.DeadLettered != 0 {
		t.Fatalf("expected no recovered jobs, got %+v", result)
	}

	stored, err := queue.GetJobByID(ctx, job)
	if err != nil {
		t.Fatalf("load fresh running job: %v", err)
	}
	if stored.Status != jobs.StatusRunning {
		t.Fatalf("expected fresh running job to remain running, got %q", stored.Status)
	}
	if stored.Attempts != 0 {
		t.Fatalf("expected attempts unchanged, got %d", stored.Attempts)
	}
}

func TestRecoverStaleRunningJobs_RespectsBatchLimit(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)
	ids := []int64{
		insertRunningJobForRecoveryTest(t, ctx, pool, jobs.WorkerPoolDefault, 0, 5, time.Now().UTC().Add(-3*time.Minute), "lost-worker-1"),
		insertRunningJobForRecoveryTest(t, ctx, pool, jobs.WorkerPoolDefault, 0, 5, time.Now().UTC().Add(-2*time.Minute), "lost-worker-2"),
		insertRunningJobForRecoveryTest(t, ctx, pool, jobs.WorkerPoolDefault, 0, 5, time.Now().UTC().Add(-1*time.Minute), "lost-worker-3"),
	}

	result, err := queue.RecoverStaleRunningJobs(ctx, jobs.WorkerPoolDefault, time.Now().UTC().Add(-30*time.Second), 2)
	if err != nil {
		t.Fatalf("recover stale jobs: %v", err)
	}
	if result.Recovered != 2 || result.DeadLettered != 0 {
		t.Fatalf("expected two recovered jobs in first batch, got %+v", result)
	}

	var pendingCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE status = 'pending'`).Scan(&pendingCount); err != nil {
		t.Fatalf("count pending jobs: %v", err)
	}
	if pendingCount != 2 {
		t.Fatalf("expected exactly two pending recovered jobs, got %d", pendingCount)
	}

	remaining, err := queue.GetJobByID(ctx, ids[2])
	if err != nil {
		t.Fatalf("load remaining running job: %v", err)
	}
	if remaining.Status != jobs.StatusRunning {
		t.Fatalf("expected one running job to remain after limited batch, got %q", remaining.Status)
	}
}

func TestRecoverStaleRunningJobs_OnlyRecoversTargetPool(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)
	defaultJob := insertRunningJobForRecoveryTest(
		t,
		ctx,
		pool,
		jobs.WorkerPoolDefault,
		0,
		5,
		time.Now().UTC().Add(-2*time.Minute),
		"lost-default-worker",
	)
	trustJob := insertRunningJobForRecoveryTest(
		t,
		ctx,
		pool,
		jobs.WorkerPoolTrust,
		0,
		5,
		time.Now().UTC().Add(-2*time.Minute),
		"lost-trust-worker",
	)

	result, err := queue.RecoverStaleRunningJobs(ctx, jobs.WorkerPoolDefault, time.Now().UTC().Add(-30*time.Second), 10)
	if err != nil {
		t.Fatalf("recover stale jobs for default pool: %v", err)
	}
	if result.Recovered != 1 || result.DeadLettered != 0 {
		t.Fatalf("unexpected recovery result: %+v", result)
	}

	defaultStored, err := queue.GetJobByID(ctx, defaultJob)
	if err != nil {
		t.Fatalf("load default pool job: %v", err)
	}
	if defaultStored.Status != jobs.StatusPending {
		t.Fatalf("expected default pool stale job to be recovered, got %q", defaultStored.Status)
	}

	trustStored, err := queue.GetJobByID(ctx, trustJob)
	if err != nil {
		t.Fatalf("load trust pool job: %v", err)
	}
	if trustStored.Status != jobs.StatusRunning {
		t.Fatalf("expected trust pool job to remain running, got %q", trustStored.Status)
	}
}

func TestRecoverStaleRunningJobs_RequiresWorkerPool(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)

	_, err := queue.RecoverStaleRunningJobs(ctx, "   ", time.Now().UTC().Add(-30*time.Second), 10)
	if err == nil {
		t.Fatal("expected error when worker pool is empty")
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

func insertTerminalJobForTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string, finishedAt time.Time) int64 {
	t.Helper()
	// Tests pre-finished_at conceptually meant "the row finished at this
	// time"; the column was conflated with updated_at then. After the
	// migration, retention purges by finished_at, so we set both to the
	// same value so the suite keeps representing the intended scenario.
	return insertTerminalJobWithFinishedAtForTest(t, ctx, pool, status, finishedAt, ptrTime(finishedAt))
}

func insertTerminalJobWithFinishedAtForTest(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	status string,
	updatedAt time.Time,
	finishedAt *time.Time,
) int64 {
	t.Helper()
	var id int64
	var finishedArg any
	if finishedAt != nil {
		finishedArg = finishedAt.UTC()
	}
	err := pool.QueryRow(ctx, `
		INSERT INTO jobs (job_type, worker_pool, payload, status, attempts, max_attempts, run_after, updated_at, finished_at)
		VALUES ('derive_profile', 'default', '{}'::jsonb, $1, 1, 5, now(), $2, $3)
		RETURNING id
	`, status, updatedAt.UTC(), finishedArg).Scan(&id)
	if err != nil {
		t.Fatalf("insert test job: %v", err)
	}
	return id
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func assertJobPresent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id int64) {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE id = $1`, id).Scan(&count); err != nil {
		t.Fatalf("count job %d: %v", id, err)
	}
	if count != 1 {
		t.Fatalf("expected job %d to remain, count=%d", id, count)
	}
}

func assertJobMissing(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id int64) {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE id = $1`, id).Scan(&count); err != nil {
		t.Fatalf("count job %d: %v", id, err)
	}
	if count != 0 {
		t.Fatalf("expected job %d to be deleted", id)
	}
}

func insertRunningJobForRecoveryTest(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	workerPool string,
	attempts int,
	maxAttempts int,
	lockedAt time.Time,
	lockedBy string,
) int64 {
	t.Helper()
	jobType := jobs.JobTypeDeriveEventBundle
	if workerPool == jobs.WorkerPoolTrust {
		jobType = jobs.JobTypeTrustComputeGlobalScore
	}
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO jobs (
			job_type,
			worker_pool,
			payload,
			status,
			attempts,
			max_attempts,
			run_after,
			locked_at,
			locked_by,
			updated_at
		)
		VALUES ($1, $2, '{}'::jsonb, $3, $4, $5, now(), $6, $7, now())
		RETURNING id
	`,
		jobType,
		workerPool,
		jobs.StatusRunning,
		attempts,
		maxAttempts,
		lockedAt.UTC(),
		lockedBy,
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert running job for stale recovery test: %v", err)
	}
	return id
}
