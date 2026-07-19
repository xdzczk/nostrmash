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
	storetrust "github.com/xdzczk/nostrmash/internal/store/trust"
)

func runTrustTargetedFetchLoop(
	ctx context.Context,
	log *slog.Logger,
	eventStore *store.PostgresStore,
	trustPrioritization config.IngestorTrustPrioritizationConfig,
	trustFetch config.IngestorTrustFetchConfig,
	relays []string,
	fetcher backfill.PageFetcher,
	onMessage backfill.MessageHandler,
) {
	if eventStore == nil || fetcher == nil || onMessage == nil {
		return
	}
	if !trustFetch.Enabled {
		return
	}
	if log == nil {
		log = slog.Default()
	}

	runCycle := func() {
		refresh, err := eventStore.RefreshTrustPubkeyFrontier(
			ctx,
			trustFetch.MaxTrackedPubkeys,
			trustFetch.StableWindow,
			trustFetch.MaxPromotionsPerCycle,
		)
		if err != nil {
			metrics.IncTrustFetchCycleOutcome("refresh_error")
			log.Warn("trust_fetch_frontier_refresh_failed", "error", err)
			return
		}
		metrics.SetTrustFetchFrontierCount("active", float64(refresh.ActiveCount))

		relayCandidates, err := eventStore.ListTrustRelayCandidates(ctx, storetrust.TrustRelayCandidateQuery{
			TopPubkeys: trustPrioritization.TopPubkeys,
			Limit:      200,
		})
		if err != nil {
			log.Warn("trust_fetch_relay_candidates_failed", "error", err)
		} else if _, err := eventStore.RefreshTrustRelaySuggestions(
			ctx,
			relayCandidates,
			trustFetch.StableWindow,
			trustFetch.MaxPromotionsPerCycle,
		); err != nil {
			log.Warn("trust_fetch_relay_suggestions_refresh_failed", "error", err)
		}

		entries, err := eventStore.ClaimTrustPubkeyFrontierForFetch(
			ctx,
			trustFetch.MaxSelectedPerCycle,
			trustFetch.FetchCooldown,
		)
		if err != nil {
			metrics.IncTrustFetchCycleOutcome("claim_error")
			log.Warn("trust_fetch_claim_failed", "error", err)
			return
		}
		if len(entries) == 0 {
			metrics.IncTrustFetchCycleOutcome("empty")
			return
		}
		metrics.AddTrustFetchPubkeysSelected(float64(len(entries)))

		successes := 0
		failures := 0
		for _, entry := range entries {
			err := fetchTrustedPubkeySlice(
				ctx,
				fetcher,
				onMessage,
				relays,
				entry.Pubkey,
				trustFetch.RecentLookbackSeconds,
				trustFetch.PageLimitPerRelay,
			)
			if err != nil {
				failures++
				metrics.IncTrustFetchPubkeyOutcome("error")
				if markErr := eventStore.MarkTrustPubkeyFetchFailure(ctx, entry.Pubkey, trustFetch.RetryDelay, err); markErr != nil {
					log.Warn("trust_fetch_mark_failure_failed", "pubkey", entry.Pubkey, "error", markErr)
				}
				continue
			}
			successes++
			metrics.IncTrustFetchPubkeyOutcome("success")
			if markErr := eventStore.MarkTrustPubkeyFetchSuccess(ctx, entry.Pubkey, trustFetch.FetchCooldown); markErr != nil {
				log.Warn("trust_fetch_mark_success_failed", "pubkey", entry.Pubkey, "error", markErr)
			}
		}

		if successes > 0 && failures == 0 {
			metrics.IncTrustFetchCycleOutcome("success")
		} else if successes > 0 && failures > 0 {
			metrics.IncTrustFetchCycleOutcome("partial")
		} else {
			metrics.IncTrustFetchCycleOutcome("error")
		}

		active, candidate, cooldown, failed, err := eventStore.GetTrustPubkeyFrontierStats(ctx)
		if err == nil {
			metrics.SetTrustFetchFrontierCount("active", float64(active))
			metrics.SetTrustFetchFrontierCount("candidate", float64(candidate))
			metrics.SetTrustFetchFrontierCount("cooldown", float64(cooldown))
			metrics.SetTrustFetchFrontierCount("failed", float64(failed))
		}
	}

	runCycle()
	ticker := time.NewTicker(trustFetch.RefreshInterval)
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

func fetchTrustedPubkeySlice(
	ctx context.Context,
	fetcher backfill.PageFetcher,
	onMessage backfill.MessageHandler,
	relays []string,
	pubkey string,
	recentLookbackSeconds int64,
	pageLimit int,
) error {
	if len(relays) == 0 {
		return fmt.Errorf("no relays configured for trust fetch")
	}
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	if pageLimit <= 0 {
		pageLimit = 200
	}

	var since *int64
	if recentLookbackSeconds > 0 {
		v := time.Now().UTC().Unix() - recentLookbackSeconds
		since = &v
	}
	kinds := []int{0, 3, 10002}
	failed := 0
	var lastErr error
	for _, relayURL := range relays {
		page, err := fetcher.FetchPage(ctx, relayURL, backfill.PageRequest{
			Kinds:   kinds,
			Authors: []string{pubkey},
			Since:   since,
			Limit:   pageLimit,
		})
		if err != nil {
			failed++
			lastErr = err
			continue
		}
		for _, payload := range page.Events {
			if err := onMessage(ctx, relayURL, payload); err != nil {
				failed++
				lastErr = err
				break
			}
		}
	}
	if failed == len(relays) && lastErr != nil {
		return fmt.Errorf("all relay fetches failed for %s: %w", pubkey, lastErr)
	}
	return nil
}
