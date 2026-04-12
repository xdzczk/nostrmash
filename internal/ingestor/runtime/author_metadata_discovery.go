package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
)

func runAuthorMetadataDiscoveryLoop(
	ctx context.Context,
	log *slog.Logger,
	eventStore *store.PostgresStore,
	cfg config.IngestorAuthorMetadataDiscoveryConfig,
	relays []string,
	fetcher backfill.PageFetcher,
	onMessage backfill.MessageHandler,
) {
	if eventStore == nil || fetcher == nil || onMessage == nil {
		return
	}
	if !cfg.Enabled {
		return
	}
	if log == nil {
		log = slog.Default()
	}

	runCycle := func() {
		pubkeys, err := eventStore.FindActiveAuthorsWithoutMetadata(ctx, cfg.BatchSize)
		if err != nil {
			log.Warn("author_metadata_discovery_query_failed", "error", err)
			metrics.IncAuthorMetadataDiscoveryOutcome("query_error")
			return
		}
		if len(pubkeys) == 0 {
			metrics.IncAuthorMetadataDiscoveryOutcome("empty")
			return
		}

		log.Info("author_metadata_discovery_batch", "candidates", len(pubkeys))
		successes := 0
		failures := 0
		for _, pubkey := range pubkeys {
			err := fetchAuthorMetadata(
				ctx,
				fetcher,
				onMessage,
				relays,
				pubkey,
				cfg.PageLimitPerRelay,
			)
			if err != nil {
				failures++
				log.Debug("author_metadata_discovery_fetch_failed",
					"pubkey", pubkey,
					"error", err,
				)
				continue
			}
			successes++
		}

		if successes > 0 && failures == 0 {
			metrics.IncAuthorMetadataDiscoveryOutcome("success")
		} else if successes > 0 {
			metrics.IncAuthorMetadataDiscoveryOutcome("partial")
		} else {
			metrics.IncAuthorMetadataDiscoveryOutcome("error")
		}
		log.Info("author_metadata_discovery_cycle_done",
			"successes", successes,
			"failures", failures,
			"batch_size", len(pubkeys),
		)
		metrics.AddAuthorMetadataDiscoveryFetched(float64(successes))
	}

	runCycle()
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runCycle()
		}
	}
}

func fetchAuthorMetadata(
	ctx context.Context,
	fetcher backfill.PageFetcher,
	onMessage backfill.MessageHandler,
	relays []string,
	pubkey string,
	pageLimit int,
) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("panic in fetchAuthorMetadata for pubkey %s: %v", pubkey, r)
		}
	}()

	if len(relays) == 0 {
		return nil
	}
	if pageLimit <= 0 {
		pageLimit = 10
	}

	kinds := []int{0}
	for _, relayURL := range relays {
		page, err := fetcher.FetchPage(ctx, relayURL, backfill.PageRequest{
			Kinds:   kinds,
			Authors: []string{pubkey},
			Limit:   pageLimit,
		})
		if err != nil {
			continue
		}
		for _, payload := range page.Events {
			if err := onMessage(ctx, relayURL, payload); err != nil {
				break
			}
		}
		if len(page.Events) > 0 {
			return nil
		}
	}
	return nil
}
