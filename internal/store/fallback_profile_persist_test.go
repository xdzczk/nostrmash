package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xdzczk/nostrmash/internal/jobs"
)

func TestPersistFallbackProfileEnqueuesCanonicalJobs(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pool)
	pp := ProfileProjection{
		Pubkey:            "fallback_pk_1",
		MetadataEventID:   "fallback_meta_1",
		MetadataCreatedAt: 1712000000,
		ProfileJSON:       json.RawMessage(`{"name":"gigi","display_name":"Gigi","about":"nostr"}`),
	}
	if err := s.PersistFallbackProfile(ctx, pp); err != nil {
		t.Fatalf("persist fallback profile: %v", err)
	}
	// Re-persisting the same profile should remain idempotent.
	if err := s.PersistFallbackProfile(ctx, pp); err != nil {
		t.Fatalf("persist fallback profile duplicate: %v", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT job_type, idempotency_key
		FROM jobs
		WHERE idempotency_key LIKE $1
		ORDER BY job_type ASC
	`, "%:fallback_meta_1")
	if err != nil {
		t.Fatalf("query jobs: %v", err)
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
		t.Fatalf("expected 3 derivation jobs for fallback event, got %d", len(got))
	}
	expected := map[string]string{
		jobs.JobTypeDeriveEventBundle:      jobs.JobTypeDeriveEventBundle + ":fallback_meta_1",
		jobs.JobTypeRepairUnresolvedRefs:   jobs.JobTypeRepairUnresolvedRefs + ":fallback_meta_1",
		jobs.JobTypeUpdateThreadProjection: jobs.JobTypeUpdateThreadProjection + ":fallback_meta_1",
	}
	for _, row := range got {
		want, ok := expected[row.jobType]
		if !ok {
			t.Fatalf("unexpected job type %q", row.jobType)
		}
		if row.key != want {
			t.Fatalf("unexpected idempotency key for %q: got %q want %q", row.jobType, row.key, want)
		}
	}

	profile, err := s.GetProfileByPubkey(ctx, pp.Pubkey)
	if err != nil {
		t.Fatalf("GetProfileByPubkey: %v", err)
	}
	if profile.MetadataEventID != pp.MetadataEventID {
		t.Fatalf("expected metadata event %q, got %q", pp.MetadataEventID, profile.MetadataEventID)
	}
}
