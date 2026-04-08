package main

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/config"
	workerruntime "github.com/xdzczk/nostrmash/internal/worker/runtime"
)

func runStaleRecoveryLoop(
	ctx context.Context,
	log interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	},
	queue workerQueue,
	workerPool string,
	cfg config.WorkerJobRecoveryConfig,
) {
	workerruntime.RunStaleRecoveryLoop(ctx, log, queue, workerPool, cfg)
}
