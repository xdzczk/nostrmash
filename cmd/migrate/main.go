// Command migrate applies all pending schema migrations and exits. It exists
// so deployments can run migrations as a dedicated pre-deploy step instead of
// inside serving processes: a boot-blocking migration inside a health-checked
// container gets killed and rolled back mid-flight (see migration 000086's
// postmortem). Serving processes then boot with MIGRATE_ON_BOOT=false and
// only wait for the schema to reach head.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/runtimebootstrap"
	"github.com/xdzczk/nostrmash/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("migrate")
	slog.SetDefault(log)

	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		log.Error("migrate_config", "error", "DATABASE_URL is required")
		os.Exit(1)
	}

	// Migrations run sequentially on one connection; a tiny pool is enough.
	pool, err := store.OpenPool(ctx, databaseURL, 2)
	if err != nil {
		log.Error("migrate_open_pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	appVersion := runtimebootstrap.ResolveAppVersion()
	log.Info("migrate_starting", "app_version", appVersion)
	if err := store.Migrate(ctx, pool, appVersion); err != nil {
		log.Error("migrate_failed", "error", err)
		os.Exit(1)
	}
	log.Info("migrate_complete")
}
