package relaydiscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

// relayCandidateAgg tracks distinct pubkey references and weighted score for a relay URL.
type relayCandidateAgg struct {
	NormalizedURL string
	URLKey        string
	DistinctUsers int
	WeightedScore float64
}

// Runner extracts relay URLs from projected user relay lists and upserts
// them into the relay registry as discovery candidates.
type Runner struct {
	log           *slog.Logger
	pool          *pgxpool.Pool
	registryStore *relayregistry.Store
	cfg           config.RelayRegistryDiscoveryConfig
	allowPrivate  bool
}

func NewRunner(
	log *slog.Logger,
	pool *pgxpool.Pool,
	registryStore *relayregistry.Store,
	cfg config.RelayRegistryDiscoveryConfig,
	allowPrivate bool,
) *Runner {
	return &Runner{
		log:           log,
		pool:          pool,
		registryStore: registryStore,
		cfg:           cfg,
		allowPrivate:  allowPrivate,
	}
}

// Run executes one discovery pass: read relay_lists_latest, aggregate distinct
// pubkey references per relay URL, and upsert candidates into the registry.
// Existing registry rows always get their user-ref counts refreshed; MaxNewCandidatesPerRun
// only limits brand-new inserts.
func (r *Runner) Run(ctx context.Context) error {
	if !r.cfg.Enabled {
		return nil
	}

	candidates, err := r.aggregateRelayReferences(ctx)
	if err != nil {
		return fmt.Errorf("aggregate relay references: %w", err)
	}

	existing, err := r.registryStore.ListURLKeys(ctx)
	if err != nil {
		return fmt.Errorf("list existing registry relays: %w", err)
	}

	limit := r.cfg.MaxNewCandidatesPerRun
	if limit <= 0 {
		limit = 25
	}

	planned := planDiscoveryUpserts(candidates, existing, r.cfg.MinDistinctUserRefs, limit)
	var refreshed, inserted int
	for _, c := range planned {
		_, alreadyTracked := existing[c.URLKey]
		if err := r.registryStore.UpsertDiscoveredRelay(
			ctx, c.URLKey, c.NormalizedURL, c.DistinctUsers, c.WeightedScore,
		); err != nil {
			r.log.Warn("relay_discovery_upsert_failed",
				"relay", c.NormalizedURL,
				"error", err,
			)
			metrics.IncRelayDiscoveryCandidates("failed")
			continue
		}
		if alreadyTracked {
			refreshed++
			metrics.IncRelayDiscoveryCandidates("refreshed")
		} else {
			inserted++
			existing[c.URLKey] = struct{}{}
			metrics.IncRelayDiscoveryCandidates("upserted")
		}
	}

	r.log.Info("relay_discovery_completed",
		"total_candidates", len(candidates),
		"refreshed", refreshed,
		"inserted", inserted,
		"min_distinct_user_refs", r.cfg.MinDistinctUserRefs,
		"max_new_candidates", limit,
	)
	return nil
}

// planDiscoveryUpserts chooses which aggregated candidates to write.
// Existing registry relays are always refreshed (including below the min-ref
// threshold, so counts can fall). New inserts must meet minDistinctUserRefs and
// are capped by maxNewInserts, preferring higher distinct-user counts first.
func planDiscoveryUpserts(
	candidates []relayCandidateAgg,
	existing map[string]struct{},
	minDistinctUserRefs int,
	maxNewInserts int,
) []relayCandidateAgg {
	if maxNewInserts < 0 {
		maxNewInserts = 0
	}
	out := make([]relayCandidateAgg, 0, len(candidates))
	newInserts := 0
	for _, c := range candidates {
		_, known := existing[c.URLKey]
		if known {
			out = append(out, c)
			continue
		}
		if c.DistinctUsers < minDistinctUserRefs {
			continue
		}
		if newInserts >= maxNewInserts {
			continue
		}
		out = append(out, c)
		newInserts++
	}
	return out
}

func (r *Runner) aggregateRelayReferences(ctx context.Context) ([]relayCandidateAgg, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT pubkey, relays_json::text
		FROM relay_lists_latest
	`)
	if err != nil {
		return nil, fmt.Errorf("query relay_lists_latest: %w", err)
	}
	defer rows.Close()

	normalizeOpts := relayurl.NormalizeOptions{RequireTLS: !r.allowPrivate}
	validateOpts := relayurl.ValidateOptions{AllowPrivateNetwork: r.allowPrivate}

	// key -> set of distinct pubkeys
	relayPubkeys := make(map[string]map[string]struct{})
	// key -> normalizedURL
	relayURLs := make(map[string]string)

	for rows.Next() {
		var pubkey, relaysText string
		if err := rows.Scan(&pubkey, &relaysText); err != nil {
			return nil, fmt.Errorf("scan relay list row: %w", err)
		}
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}

		var relays []string
		if err := json.Unmarshal([]byte(relaysText), &relays); err != nil {
			continue
		}

		for _, raw := range relays {
			normalized, err := relayurl.Normalize(raw, normalizeOpts)
			if err != nil {
				continue
			}
			if err := relayurl.Validate(normalized, validateOpts); err != nil {
				continue
			}
			urlKey := relayurl.CanonicalKey(normalized)
			if _, ok := relayPubkeys[urlKey]; !ok {
				relayPubkeys[urlKey] = make(map[string]struct{})
				relayURLs[urlKey] = normalized
			}
			relayPubkeys[urlKey][pubkey] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read relay list rows: %w", err)
	}

	candidates := make([]relayCandidateAgg, 0, len(relayPubkeys))
	for urlKey, pubkeys := range relayPubkeys {
		candidates = append(candidates, relayCandidateAgg{
			NormalizedURL: relayURLs[urlKey],
			URLKey:        urlKey,
			DistinctUsers: len(pubkeys),
			WeightedScore: float64(len(pubkeys)),
		})
	}

	// Sort by distinct user count descending so new-insert budget prefers popular relays.
	sortCandidates(candidates)
	return candidates, nil
}

func sortCandidates(candidates []relayCandidateAgg) {
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && candidates[j].DistinctUsers > candidates[j-1].DistinctUsers; j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}
}
