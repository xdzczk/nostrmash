package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	apptrustworker "github.com/xdzczk/nostrmash/internal/app/trustworker"
	"github.com/xdzczk/nostrmash/internal/logging"
)

var (
	buildVersion = ""
	buildCommit  = "unknown"
	buildTime    = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("trust_worker")
	slog.SetDefault(log)

	if err := apptrustworker.Run(ctx, log, apptrustworker.BuildInfo{
		Version: buildVersion,
		Commit:  buildCommit,
		Time:    buildTime,
	}); err != nil {
		log.Error("trust_worker_run", "error", err)
		os.Exit(1)
	}
}
