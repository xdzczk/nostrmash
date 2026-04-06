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
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

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
		SELECT job_type, idempotency_key
		FROM jobs
		ORDER BY job_type ASC
	`)
	if err != nil {
		t.Fatalf("query enqueued jobs: %v", err)
	}
	defer rows.Close()

	type row struct {
		jobType string
		key     string
	}
	got := make([]row, 0)
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.jobType, &r.key); err != nil {
			t.Fatalf("scan jobs row: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read jobs rows: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 derivation jobs, got %d", len(got))
	}
	expected := map[string]string{
		jobs.JobTypeDeriveEventBundle:      jobs.JobTypeDeriveEventBundle + ":event_enqueue_1",
		jobs.JobTypeRepairUnresolvedRefs:   jobs.JobTypeRepairUnresolvedRefs + ":event_enqueue_1",
		jobs.JobTypeUpdateThreadProjection: jobs.JobTypeUpdateThreadProjection + ":event_enqueue_1",
	}
	for _, row := range got {
		key, ok := expected[row.jobType]
		if !ok {
			t.Fatalf("unexpected job type %q", row.jobType)
		}
		if row.key != key {
			t.Fatalf("unexpected idempotency key for %q: got %q want %q", row.jobType, row.key, key)
		}
	}
}
