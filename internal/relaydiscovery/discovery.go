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
func (r *Runner) Run(ctx context.Context) error {
	if !r.cfg.Enabled {
		return nil
	}

	candidates, err := r.aggregateRelayReferences(ctx)
	if err != nil {
		return fmt.Errorf("aggregate relay references: %w", err)
	}

	var upserted int
	limit := r.cfg.MaxNewCandidatesPerRun
	if limit <= 0 {
		limit = 25
	}

	for _, c := range candidates {
		if c.DistinctUsers < r.cfg.MinDistinctUserRefs {
			continue
		}
		if upserted >= limit {
			break
		}
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
		upserted++
		metrics.IncRelayDiscoveryCandidates("upserted")
	}

	r.log.Info("relay_discovery_completed",
		"total_candidates", len(candidates),
		"upserted", upserted,
		"min_distinct_user_refs", r.cfg.MinDistinctUserRefs,
	)
	return nil
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

	// Sort by distinct user count descending for budget-limited upserts.
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
