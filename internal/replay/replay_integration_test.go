package replay_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/replay"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

func TestReplayFixtureMatchesGoldenSnapshot(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	runner, err := replay.NewRunner(nil, pool, replayValidationOptions())
	if err != nil {
		t.Fatalf("new replay runner: %v", err)
	}

	fixturePath := "testdata/relay_payloads/basic_flow.ndjson"
	result, err := runner.ReplayFixturePath(ctx, fixturePath)
	if err != nil {
		t.Fatalf("replay fixture: %v", err)
	}
	if result.EntriesReplayed != 3 {
		t.Fatalf("unexpected replay entry count: got=%d want=3", result.EntriesReplayed)
	}
	if result.JobsProcessed != 9 {
		t.Fatalf("unexpected processed jobs: got=%d want=9", result.JobsProcessed)
	}

	got, err := replay.CaptureStateSnapshot(ctx, pool)
	if err != nil {
		t.Fatalf("capture replay state snapshot: %v", err)
	}
	want := readGoldenSnapshot(t, "testdata/golden/basic_flow_snapshot.json")
	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		wantJSON, _ := json.MarshalIndent(want, "", "  ")
		t.Fatalf("snapshot mismatch\n--- got ---\n%s\n--- want ---\n%s", gotJSON, wantJSON)
	}
}

func TestReplayIsDeterministicAcrossFreshSchemas(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	fixturePath := "testdata/relay_payloads/basic_flow.ndjson"

	first := runReplayAndSnapshot(t, ctx, dbURL, fixturePath)
	second := runReplayAndSnapshot(t, ctx, dbURL, fixturePath)
	if !reflect.DeepEqual(first, second) {
		firstJSON, _ := json.MarshalIndent(first, "", "  ")
		secondJSON, _ := json.MarshalIndent(second, "", "  ")
		t.Fatalf("replay snapshots differ between runs\n--- first ---\n%s\n--- second ---\n%s", firstJSON, secondJSON)
	}
}

func runReplayAndSnapshot(t *testing.T, ctx context.Context, dbURL, fixturePath string) replay.StateSnapshot {
	t.Helper()
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	runner, err := replay.NewRunner(nil, pool, replayValidationOptions())
	if err != nil {
		t.Fatalf("new replay runner: %v", err)
	}
	if _, err := runner.ReplayFixturePath(ctx, fixturePath); err != nil {
		t.Fatalf("replay fixture: %v", err)
	}
	snapshot, err := replay.CaptureStateSnapshot(ctx, pool)
	if err != nil {
		t.Fatalf("capture replay state snapshot: %v", err)
	}
	return snapshot
}

func replayValidationOptions() nostr.Options {
	return nostr.Options{}
}

func readGoldenSnapshot(t *testing.T, path string) replay.StateSnapshot {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden file: %v", err)
	}
	var snapshot replay.StateSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("decode golden file: %v", err)
	}
	return snapshot
}

func setupSchemaPool(t *testing.T, ctx context.Context, dbURL string) *pgxpool.Pool {
	t.Helper()
	return dbtest.SetupSchemaPool(t, ctx, dbURL, "replay")
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	return dbtest.DatabaseURL(t, "replay")
}
