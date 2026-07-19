package trust

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

type TrustRelayCandidate struct {
	RelayURL                string
	WeightedScore           float64
	SupportingPubkeysCount  int64
	SupportingPubkeysSample []string
	SourceRunID             *int64
	SourceComputedAt        *time.Time
}

type TrustRelayCandidateQuery struct {
	TopPubkeys       int
	Limit            int
	ConfiguredOnly   bool
	ConfiguredRelays []string
}

type TrustPubkeyCandidate struct {
	Pubkey      string
	Score       float64
	Rank        int64
	SourceRunID int64
	ComputedAt  time.Time
}

func NormalizeRelayURLs(relays []string) ([]string, map[string]int) {
	baseOrder := make(map[string]int, len(relays))
	normalized := make([]string, 0, len(relays))
	for idx, relayURL := range relays {
		relayURL = strings.TrimSpace(strings.ToLower(relayURL))
		if relayURL == "" {
			continue
		}
		if _, exists := baseOrder[relayURL]; exists {
			continue
		}
		baseOrder[relayURL] = idx
		normalized = append(normalized, relayURL)
	}
	return normalized, baseOrder
}

func SortRelaysByWeights(normalized []string, baseOrder map[string]int, weights map[string]float64) []string {
	sorted := append([]string(nil), normalized...)
	sort.SliceStable(sorted, func(i, j int) bool {
		left := sorted[i]
		right := sorted[j]
		lw := weights[left]
		rw := weights[right]
		if lw == rw {
			return baseOrder[left] < baseOrder[right]
		}
		return lw > rw
	})
	return sorted
}

func (s *Trust) ListTrustRelayCandidates(
	ctx context.Context,
	query TrustRelayCandidateQuery,
) ([]TrustRelayCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if query.TopPubkeys <= 0 {
		query.TopPubkeys = 2000
	}
	if query.Limit <= 0 {
		query.Limit = 100
	}
	configuredRelays, _ := NormalizeRelayURLs(query.ConfiguredRelays)
	rows, err := s.pool.Query(ctx, `
		WITH trusted AS (
			SELECT pubkey, score, run_id, computed_at
			FROM trust_scores_global
			ORDER BY rank ASC
			LIMIT $1
		),
		relay_votes AS (
			SELECT
				lower(trim(jsonb_array_elements_text(rl.relays_json)::text)) AS relay_url,
				t.pubkey,
				t.score,
				t.run_id,
				t.computed_at
			FROM trusted t
			JOIN relay_lists_latest rl ON rl.pubkey = t.pubkey
		),
		filtered AS (
			SELECT relay_url, pubkey, score, run_id, computed_at
			FROM relay_votes
			WHERE relay_url <> ''
			  AND (NOT $2::boolean OR relay_url = ANY($3))
		),
		aggregated AS (
			SELECT
				relay_url,
				SUM(score) AS weighted_score,
				COUNT(DISTINCT pubkey) AS supporting_pubkeys_count,
				MAX(run_id) AS source_run_id,
				MAX(computed_at) AS source_computed_at
			FROM filtered
			GROUP BY relay_url
		)
		SELECT
			a.relay_url,
			a.weighted_score,
			a.supporting_pubkeys_count,
			a.source_run_id,
			a.source_computed_at,
			COALESCE(ARRAY(
				SELECT ranked.pubkey
				FROM (
					SELECT f2.pubkey, MAX(f2.score) AS max_score
					FROM filtered f2
					WHERE f2.relay_url = a.relay_url
					GROUP BY f2.pubkey
					ORDER BY MAX(f2.score) DESC, f2.pubkey ASC
					LIMIT 3
				) AS ranked
			), ARRAY[]::text[]) AS supporting_pubkeys_sample
		FROM aggregated a
		ORDER BY a.weighted_score DESC, a.relay_url ASC
		LIMIT $4
	`, query.TopPubkeys, query.ConfiguredOnly, configuredRelays, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("list trust relay candidates: %w", err)
	}
	defer rows.Close()

	out := make([]TrustRelayCandidate, 0, query.Limit)
	for rows.Next() {
		var item TrustRelayCandidate
		if err := rows.Scan(
			&item.RelayURL,
			&item.WeightedScore,
			&item.SupportingPubkeysCount,
			&item.SourceRunID,
			&item.SourceComputedAt,
			&item.SupportingPubkeysSample,
		); err != nil {
			return nil, fmt.Errorf("scan trust relay candidate: %w", err)
		}
		item.RelayURL = strings.TrimSpace(strings.ToLower(item.RelayURL))
		if item.SourceComputedAt != nil {
			ts := item.SourceComputedAt.UTC()
			item.SourceComputedAt = &ts
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust relay candidate rows: %w", err)
	}
	return out, nil
}

func (s *Trust) PrioritizeConfiguredRelaysByTrust(
	ctx context.Context,
	relays []string,
	topPubkeys int,
) ([]string, error) {
	normalized, baseOrder := NormalizeRelayURLs(relays)
	if len(normalized) == 0 {
		return []string{}, nil
	}
	candidates, err := s.ListTrustRelayCandidates(ctx, TrustRelayCandidateQuery{
		TopPubkeys:       topPubkeys,
		Limit:            len(normalized),
		ConfiguredOnly:   true,
		ConfiguredRelays: normalized,
	})
	if err != nil {
		return nil, err
	}
	weights := make(map[string]float64, len(candidates))
	for _, candidate := range candidates {
		weights[candidate.RelayURL] = candidate.WeightedScore
	}
	sorted := SortRelaysByWeights(normalized, baseOrder, weights)
	if len(sorted) == 0 {
		return slices.Clone(normalized), nil
	}
	return sorted, nil
}

func (s *Trust) ListTrustPubkeyCandidates(ctx context.Context, limit int) ([]TrustPubkeyCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 2000
	}
	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, score, rank, run_id, computed_at
		FROM trust_scores_global
		ORDER BY rank ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list trust pubkey candidates: %w", err)
	}
	defer rows.Close()
	out := make([]TrustPubkeyCandidate, 0, limit)
	for rows.Next() {
		var item TrustPubkeyCandidate
		if err := rows.Scan(
			&item.Pubkey,
			&item.Score,
			&item.Rank,
			&item.SourceRunID,
			&item.ComputedAt,
		); err != nil {
			return nil, fmt.Errorf("scan trust pubkey candidate: %w", err)
		}
		item.Pubkey = strings.TrimSpace(item.Pubkey)
		item.ComputedAt = item.ComputedAt.UTC()
		if item.Pubkey == "" {
			continue
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trust pubkey candidates: %w", err)
	}
	return out, nil
}
