package dbtest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabaseURL returns TEST_DATABASE_URL or DATABASE_URL and skips the test if unset.
func DatabaseURL(t testing.TB, suiteName string) string {
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

	t.Skipf("set TEST_DATABASE_URL or DATABASE_URL to run %s integration tests", suiteName)
	return ""
}

// SetupSchemaPool creates an isolated schema-scoped pool and drops the schema in cleanup.
func SetupSchemaPool(t testing.TB, ctx context.Context, dbURL, schemaPrefix string) *pgxpool.Pool {
	t.Helper()

	adminPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schemaName := fmt.Sprintf("test_%s_%d", schemaPrefix, time.Now().UnixNano())
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
