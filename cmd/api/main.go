package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	appapi "github.com/xdzczk/nostrmash/internal/app/api"
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

	log := logging.New("api")
	slog.SetDefault(log)

	if err := appapi.Run(ctx, log, appapi.BuildInfo{
		Version: buildVersion,
		Commit:  buildCommit,
		Time:    buildTime,
	}, stop); err != nil {
		log.Error("api_run", "error", err)
		os.Exit(1)
	}
}
