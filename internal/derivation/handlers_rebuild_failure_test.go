package derivation_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectionRebuildRun_FailedRunsTrackErrorAndRetryAttempts(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationReplyCounts,
		TargetVersion:  2,
		Scope: derivation.ProjectionRebuildScope{
			Type:    derivation.RebuildScopeEvent,
			EventID: "missing_event",
		},
	})
	if err != nil {
		t.Fatalf("trigger missing-event rebuild: %v", err)
	}

	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err == nil {
		t.Fatalf("expected first rebuild attempt to fail")
	}
	first, err := handlers.GetProjectionRebuildRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after first failure: %v", err)
	}
	if first.Status != derivation.RebuildStatusFailed {
		t.Fatalf("expected failed status after first attempt, got %q", first.Status)
	}
	if first.Attempts != 1 {
		t.Fatalf("expected attempts=1 after first failure, got %d", first.Attempts)
	}
	if first.StartedAt == nil || first.FinishedAt == nil {
		t.Fatalf("expected started_at and finished_at to be set after first failure")
	}
	if first.LastError == nil || strings.TrimSpace(*first.LastError) == "" {
		t.Fatalf("expected last_error to be populated after first failure")
	}

	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err == nil {
		t.Fatalf("expected second rebuild attempt to fail")
	}
	second, err := handlers.GetProjectionRebuildRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after second failure: %v", err)
	}
	if second.Status != derivation.RebuildStatusFailed {
		t.Fatalf("expected failed status after second attempt, got %q", second.Status)
	}
	if second.Attempts != 2 {
		t.Fatalf("expected attempts=2 after second failure, got %d", second.Attempts)
	}
	if second.LastError == nil || strings.TrimSpace(*second.LastError) == "" {
		t.Fatalf("expected last_error to remain populated after second failure")
	}
}
