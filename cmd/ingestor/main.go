package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/xdzczk/nostrmash/internal/config"
	ingestorruntime "github.com/xdzczk/nostrmash/internal/ingestor/runtime"
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

	log := logging.New("ingestor")
	slog.SetDefault(log)
	if err := runIngestor(ctx, log); err != nil {
		os.Exit(1)
	}
}

func runIngestor(ctx context.Context, log *slog.Logger) error {
	cfg, err := config.LoadIngestor()
	if err != nil {
		log.Error("config", "error", err)
		return fmt.Errorf("load ingestor config: %w", err)
	}
	return ingestorruntime.Run(ctx, log, cfg, ingestorruntime.BuildInfo{
		Version: buildVersion,
		Commit:  buildCommit,
		Time:    buildTime,
	})
}
