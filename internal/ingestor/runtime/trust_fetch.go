package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/relaylookup"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/store"
	storetrust "github.com/xdzczk/nostrmash/internal/store/trust"
)

// authorWriteRelayPolicy is the dial gate for relays taken from an author's
// own relay list. Disabled relays are operator exclusions. requireTLS drops
// ws:// URLs when the ingestor is not in local dev.
type authorWriteRelayPolicy struct {
	disabled   []string
	requireTLS bool
}

func runTrustTargetedFetchLoop(
	ctx context.Context,
	log *slog.Logger,
	eventStore *store.PostgresStore,
	trustPrioritization config.IngestorTrustPrioritizationConfig,
	trustFetch config.IngestorTrustFetchConfig,
	relays []string,
	writeRelays authorWriteRelayPolicy,
	registry *relayregistry.Store,
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
			noteRelays, noteErr := extraAuthorWriteRelays(ctx, eventStore, registry, entry.Pubkey, relays, writeRelays)
			if noteErr != nil {
				log.Warn("trust_fetch_write_relays_failed", "pubkey", entry.Pubkey, "error", noteErr)
				noteRelays = nil
			} else if len(noteRelays) > 0 {
				log.Debug("trust_fetch_write_relays", "pubkey", entry.Pubkey, "relays", noteRelays)
			}
			err := fetchTrustedPubkeySlice(
				ctx,
				fetcher,
				onMessage,
				relays,
				noteRelays,
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

// extraAuthorWriteRelays returns up to MaxExtraAuthorWriteRelays of pubkey's
// write relays that are not already in base. A relay is included only when
// the registry's latest probe reached EOSE. Lookup failures return an error
// so the caller can keep the base relays and still fetch the pubkey.
func extraAuthorWriteRelays(
	ctx context.Context,
	eventStore *store.PostgresStore,
	registry *relayregistry.Store,
	pubkey string,
	base []string,
	policy authorWriteRelayPolicy,
) ([]string, error) {
	if eventStore == nil || registry == nil {
		return nil, nil
	}
	kind10002, err := eventStore.LatestKind10002Raw(ctx, pubkey)
	if err != nil {
		return nil, err
	}
	kind3, err := eventStore.LatestKind3Raw(ctx, pubkey)
	if err != nil {
		return nil, err
	}
	candidates := nostr.NormalizedAuthorWriteRelays(kind10002, kind3, policy.requireTLS)
	if len(candidates) == 0 {
		return nil, nil
	}
	healthy, err := registry.ProbeOKRelaySet(ctx, candidates)
	if err != nil {
		return nil, err
	}
	extras := nostr.PickExtraWriteRelays(candidates, base, policy.disabled, healthy, policy.requireTLS, nostr.MaxExtraAuthorWriteRelays)
	return relaylookup.WithoutDirectoryRelays(extras), nil
}

func fetchTrustedPubkeySlice(
	ctx context.Context,
	fetcher backfill.PageFetcher,
	onMessage backfill.MessageHandler,
	relays []string,
	noteRelays []string,
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
	// Notes and replaceables are separate requests. A single REQ shares one
	// limit across every kind, so a busy author's kind-1 page used to crowd
	// out profile, follow, and relay-list events (and a metadata-only request
	// never repaired a missed note).
	succeeded := 0
	var lastErr error
	for _, kinds := range trustFetchKindGroups {
		// Notes are the events a write-relay gap drops. Replaceables and
		// lists stay on the ingest relays; asking every write relay for
		// them would multiply the refetch by the whole kind list.
		targets := relays
		if len(kinds) > 0 && kinds[0] == 1 {
			targets = append(append([]string{}, relays...), noteRelays...)
		}
		for _, relayURL := range targets {
			page, err := fetcher.FetchPage(ctx, relayURL, backfill.PageRequest{
				Kinds:   kinds,
				Authors: []string{pubkey},
				Since:   since,
				Limit:   pageLimit,
			})
			if err != nil {
				lastErr = err
				continue
			}
			delivered := true
			for _, payload := range page.Events {
				if err := onMessage(ctx, relayURL, payload); err != nil {
					lastErr = err
					delivered = false
					break
				}
			}
			if delivered {
				succeeded++
			}
		}
	}
	if succeeded == 0 && lastErr != nil {
		return fmt.Errorf("all relay fetches failed for %s: %w", pubkey, lastErr)
	}
	return nil
}

// trustFetchKindGroups is the order of author-scoped REQ filters. Notes come
// first so a process that stops mid-pubkey has already asked for the posts
// people are missing.
var trustFetchKindGroups = [][]int{
	{1, 30023},
	{0, 3, 10002},
	{20, 21, 22, 34235, 34236, 30311},
	{10000, 10001, 10003, 30000},
}
