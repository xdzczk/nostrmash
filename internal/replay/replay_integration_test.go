package replay_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/replay"
	"github.com/xdzczk/nostrmash/internal/store"
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

	adminPool, err := store.OpenPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schemaName := fmt.Sprintf("test_replay_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, quotedSchema)); err != nil {
		adminPool.Close()
		t.Fatalf("create schema %s: %v", schemaName, err)
	}

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		adminPool.Close()
		t.Fatalf("parse pool config: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schemaName

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		adminPool.Close()
		t.Fatalf("open schema pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, quotedSchema))
		adminPool.Close()
	})

	return pool
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()

	candidates := []string{
		os.Getenv("TEST_DATABASE_URL"),
		os.Getenv("DATABASE_URL"),
	}
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		return candidate
	}

	t.Skip("set TEST_DATABASE_URL or DATABASE_URL to run replay integration tests")
	return ""
}
