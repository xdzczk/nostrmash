package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/model"
)

func TestInsertCanonicalEventEnqueuesDerivationJobsOnce(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	event := model.Event{
		ID:          "event_enqueue_1",
		Pubkey:      "pub_enqueue",
		CreatedAt:   123,
		Kind:        1,
		Sig:         "sig_enqueue",
		Content:     "enqueue",
		RawJSON:     json.RawMessage(`{"id":"event_enqueue_1","kind":1,"tags":[]}`),
		FirstSeenAt: time.Date(2026, 4, 4, 15, 0, 0, 0, time.UTC),
		InsertedAt:  time.Date(2026, 4, 4, 15, 0, 0, 0, time.UTC),
	}
	if err := s.InsertCanonicalEvent(ctx, event, nil, "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	// Duplicate should not enqueue duplicate derivation jobs.
	if err := s.InsertCanonicalEvent(ctx, event, nil, "wss://relay.two", event.FirstSeenAt.Add(1*time.Second)); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT job_type, idempotency_key, worker_pool
		FROM jobs
		ORDER BY job_type ASC
	`)
	if err != nil {
		t.Fatalf("query enqueued jobs: %v", err)
	}
	defer rows.Close()

	type row struct {
		jobType    string
		key        string
		workerPool string
	}
	got := make([]row, 0)
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.jobType, &r.key, &r.workerPool); err != nil {
			t.Fatalf("scan jobs row: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read jobs rows: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected exactly 1 composite derivation job, got %d (%+v)", len(got), got)
	}
	if got[0].jobType != jobs.JobTypeDeriveEventBundle {
		t.Fatalf("unexpected job type: got %q want %q", got[0].jobType, jobs.JobTypeDeriveEventBundle)
	}
	wantKey := jobs.JobTypeDeriveEventBundle + ":event_enqueue_1"
	if got[0].key != wantKey {
		t.Fatalf("unexpected idempotency key: got %q want %q", got[0].key, wantKey)
	}
	if got[0].workerPool != jobs.WorkerPoolDefault {
		t.Fatalf("unexpected worker pool when no override: got %q want %q", got[0].workerPool, jobs.WorkerPoolDefault)
	}
}

func TestInsertCanonicalEventRoutesToContextWorkerPool(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	cases := []struct {
		name     string
		eventID  string
		pool     string
		expected string
	}{
		{name: "live", eventID: "event_pool_live", pool: jobs.WorkerPoolLive, expected: jobs.WorkerPoolLive},
		{name: "backfill", eventID: "event_pool_backfill", pool: jobs.WorkerPoolBackfill, expected: jobs.WorkerPoolBackfill},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := model.Event{
				ID:          tc.eventID,
				Pubkey:      "pub_pool",
				CreatedAt:   123,
				Kind:        1,
				Sig:         "sig_pool",
				Content:     "pool",
				RawJSON:     json.RawMessage(`{"id":"` + tc.eventID + `","kind":1,"tags":[]}`),
				FirstSeenAt: time.Date(2026, 4, 4, 15, 0, 0, 0, time.UTC),
				InsertedAt:  time.Date(2026, 4, 4, 15, 0, 0, 0, time.UTC),
			}
			poolCtx := jobs.WithWorkerPool(ctx, tc.pool)
			if err := s.InsertCanonicalEvent(poolCtx, event, nil, "wss://relay", event.FirstSeenAt); err != nil {
				t.Fatalf("insert: %v", err)
			}

			var workerPool string
			if err := pool.QueryRow(ctx, `
				SELECT worker_pool FROM jobs
				WHERE idempotency_key = $1
			`, jobs.JobTypeDeriveEventBundle+":"+tc.eventID).Scan(&workerPool); err != nil {
				t.Fatalf("query worker pool: %v", err)
			}
			if workerPool != tc.expected {
				t.Fatalf("unexpected worker pool: got %q want %q", workerPool, tc.expected)
			}
		})
	}
}
