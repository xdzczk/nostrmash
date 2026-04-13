package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/logging"
	workerruntime "github.com/xdzczk/nostrmash/internal/worker/runtime"
)

var (
	buildVersion = ""
	buildCommit  = "unknown"
	buildTime    = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("worker")
	slog.SetDefault(log)
	if err := runWorker(ctx, log); err != nil {
		os.Exit(1)
	}
}

func runWorker(ctx context.Context, log interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}) error {
	cfg, err := config.LoadWorker()
	if err != nil {
		log.Error("config", "error", err)
		return fmt.Errorf("load worker config: %w", err)
	}
	return workerruntime.Run(ctx, log, cfg, workerruntime.BuildInfo{
		Version: buildVersion,
		Commit:  buildCommit,
		Time:    buildTime,
	}, func(
		loopCtx context.Context,
		loopLog workerruntime.Logger,
		queue workerruntime.Queue,
		workerID string,
		workerPool string,
		batchSize int,
		concurrency int,
		pollInterval time.Duration,
		retryDelay time.Duration,
		processJob workerruntime.ProcessJobFn,
	) {
		runClaimLoop(
			loopCtx,
			loopLog,
			queue,
			workerID,
			workerPool,
			batchSize,
			concurrency,
			pollInterval,
			retryDelay,
			processJob,
		)
	})
}
