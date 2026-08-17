package appapi

import (
	"context"
	"log/slog"
	"time"

	"github.com/xdzczk/nostrmash/internal/relaylookup"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

type eventFallbackRanker interface {
	ListFastHealthyLookupRelays(ctx context.Context, limit int) ([]string, error)
}

var _ eventFallbackRanker = (*relayregistry.Store)(nil)

func runEventFallbackRefreshLoop(
	ctx context.Context,
	log *slog.Logger,
	client *relaylookup.Client,
	ranker eventFallbackRanker,
	staticURLs []string,
	fanout int,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshEventFallbackRelays(ctx, log, client, ranker, staticURLs, fanout)
		}
	}
}

func refreshEventFallbackRelays(
	ctx context.Context,
	log *slog.Logger,
	client *relaylookup.Client,
	ranker eventFallbackRanker,
	staticURLs []string,
	fanout int,
) {
	if client == nil || ranker == nil {
		return
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ranked, err := ranker.ListFastHealthyLookupRelays(refreshCtx, fanout)
	if err != nil {
		log.Error("relay_fallback_registry_refresh_failed", "error", err)
		return
	}
	merged := relaylookup.MergeEventFallbackRelays(ranked, staticURLs, fanout)
	client.SetEventRelays(merged)
	log.Info("relay_fallback_event_relays", "relays", merged, "ranked_count", len(ranked))
}
