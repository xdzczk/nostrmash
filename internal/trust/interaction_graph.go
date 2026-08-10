package trust

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	interactionWeightReaction = 1.0
	interactionWeightRepost   = 2.0
	interactionWeightReply    = 1.5
)

// RefreshInteractionEdgeWeights rebuilds trust_interaction_edge_weights from
// engagement projections. Full refresh keeps the table rebuildable and avoids
// an event-driven incremental path until operators opt into the graph.
func RefreshInteractionEdgeWeights(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	if pool == nil {
		return 0, fmt.Errorf("postgres pool is required")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin interaction edge refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"SET LOCAL statement_timeout = %d",
		trustEdgeScanStatementTimeout.Milliseconds(),
	)); err != nil {
		return 0, fmt.Errorf("set interaction edge statement_timeout: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM trust_interaction_edge_weights`); err != nil {
		return 0, fmt.Errorf("clear interaction edge weights: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO trust_interaction_edge_weights (src_pubkey, dst_pubkey, weight, updated_at)
		SELECT src_pubkey, dst_pubkey, SUM(weight) AS weight, now()
		FROM (
			SELECT
				r.reactor_pubkey AS src_pubkey,
				e.pubkey AS dst_pubkey,
				$1::double precision AS weight
			FROM reaction_events r
			INNER JOIN events e ON e.id = r.target_event_id
			WHERE r.reactor_pubkey <> e.pubkey

			UNION ALL

			SELECT
				r.reposter_pubkey AS src_pubkey,
				e.pubkey AS dst_pubkey,
				$2::double precision AS weight
			FROM repost_events r
			INNER JOIN events e ON e.id = r.target_event_id
			WHERE r.reposter_pubkey <> e.pubkey

			UNION ALL

			SELECT
				z.sender_pubkey AS src_pubkey,
				COALESCE(z.receiver_pubkey, e.pubkey) AS dst_pubkey,
				LN(1 + GREATEST(z.amount_sats, 0)::double precision) AS weight
			FROM zap_receipts z
			LEFT JOIN events e ON e.id = z.event_id
			WHERE COALESCE(z.receiver_pubkey, e.pubkey) IS NOT NULL
			  AND z.sender_pubkey <> COALESCE(z.receiver_pubkey, e.pubkey)

			UNION ALL

			SELECT
				child.pubkey AS src_pubkey,
				parent.pubkey AS dst_pubkey,
				$3::double precision AS weight
			FROM thread_edges te
			INNER JOIN events child ON child.id = te.child_event_id
			INNER JOIN events parent ON parent.id = te.parent_event_id
			WHERE NOT te.parent_missing
			  AND child.pubkey <> parent.pubkey
		) edges
		WHERE src_pubkey <> '' AND dst_pubkey <> ''
		GROUP BY src_pubkey, dst_pubkey
		HAVING SUM(weight) > 0
	`, interactionWeightReaction, interactionWeightRepost, interactionWeightReply)
	if err != nil {
		return 0, fmt.Errorf("rebuild interaction edge weights: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit interaction edge refresh: %w", err)
	}
	return tag.RowsAffected(), nil
}

func loadInteractionWeights(ctx context.Context, pool *pgxpool.Pool) (map[string]map[string]float64, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres pool is required")
	}
	rows, err := pool.Query(ctx, `
		SELECT src_pubkey, dst_pubkey, weight
		FROM trust_interaction_edge_weights
	`)
	if err != nil {
		return nil, fmt.Errorf("load interaction edge weights: %w", err)
	}
	defer rows.Close()

	out := make(map[string]map[string]float64)
	for rows.Next() {
		var src, dst string
		var weight float64
		if err := rows.Scan(&src, &dst, &weight); err != nil {
			return nil, fmt.Errorf("scan interaction edge weight: %w", err)
		}
		src = strings.TrimSpace(src)
		dst = strings.TrimSpace(dst)
		if src == "" || dst == "" || weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
			continue
		}
		if out[src] == nil {
			out[src] = make(map[string]float64)
		}
		out[src][dst] += weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read interaction edge weights: %w", err)
	}
	return out, nil
}

func adjacencyToWeighted(adjacency map[string][]string) map[string]map[string]float64 {
	out := make(map[string]map[string]float64, len(adjacency))
	for src, dsts := range adjacency {
		src = strings.TrimSpace(src)
		if src == "" {
			continue
		}
		bucket := out[src]
		if bucket == nil {
			bucket = make(map[string]float64, len(dsts))
			out[src] = bucket
		}
		for _, dst := range dsts {
			dst = strings.TrimSpace(dst)
			if dst == "" {
				continue
			}
			bucket[dst] += 1
		}
	}
	return out
}

func mergeWeightedAdjacency(
	base map[string]map[string]float64,
	extra map[string]map[string]float64,
	nodeSet map[string]struct{},
) map[string]map[string]float64 {
	if base == nil {
		base = make(map[string]map[string]float64)
	}
	for src, dsts := range extra {
		src = strings.TrimSpace(src)
		if src == "" {
			continue
		}
		nodeSet[src] = struct{}{}
		bucket := base[src]
		if bucket == nil {
			bucket = make(map[string]float64, len(dsts))
			base[src] = bucket
		}
		for dst, weight := range dsts {
			dst = strings.TrimSpace(dst)
			if dst == "" || weight <= 0 {
				continue
			}
			bucket[dst] += weight
			nodeSet[dst] = struct{}{}
		}
	}
	return base
}

// InteractionRankComparison summarizes follow-only vs follow+interaction ranks.
type InteractionRankComparison struct {
	NodeCount            int      `json:"node_count"`
	InteractionEdgeCount int      `json:"interaction_edge_count"`
	TopN                 int      `json:"top_n"`
	TopNOverlap          int      `json:"top_n_overlap"`
	TopNOverlapRatio     float64  `json:"top_n_overlap_ratio"`
	SpearmanCorrelation  float64  `json:"spearman_correlation"`
	FollowOnlyTop        []string `json:"follow_only_top"`
	FollowInteractionTop []string `json:"follow_interaction_top"`
}

// CompareFollowAndInteractionRanks computes an operator-facing comparison of
// follow-only PageRank versus follow+interaction PageRank on the current graph.
func CompareFollowAndInteractionRanks(ctx context.Context, pool *pgxpool.Pool, topN int) (InteractionRankComparison, error) {
	if topN <= 0 {
		topN = 100
	}
	adjacency, nodeSet, err := LoadAdjacencyFromPostgres(ctx, pool)
	if err != nil {
		return InteractionRankComparison{}, err
	}
	interaction, err := loadInteractionWeights(ctx, pool)
	if err != nil {
		return InteractionRankComparison{}, err
	}
	followOnly := computeIterativeGlobalRank(adjacency, cloneNodeSet(nodeSet))
	mergedNodes := cloneNodeSet(nodeSet)
	mergedWeighted := mergeWeightedAdjacency(adjacencyToWeighted(adjacency), interaction, mergedNodes)
	followPlus := ComputePersonalizedRankWeighted(
		mergedWeighted,
		mergedNodes,
		uniformTeleport(mergedNodes),
		rankDamping,
	)

	edgeCount := 0
	for _, dsts := range interaction {
		edgeCount += len(dsts)
	}
	followTop := topPubkeys(followOnly, topN)
	mergedTop := topPubkeys(followPlus, topN)
	overlap := setOverlap(followTop, mergedTop)
	corr := spearmanRankCorrelation(followOnly, followPlus)
	ratio := 0.0
	denom := min(topN, len(followTop), len(mergedTop))
	if denom > 0 {
		ratio = float64(overlap) / float64(denom)
	}
	return InteractionRankComparison{
		NodeCount:            len(nodeSet),
		InteractionEdgeCount: edgeCount,
		TopN:                 topN,
		TopNOverlap:          overlap,
		TopNOverlapRatio:     ratio,
		SpearmanCorrelation:  corr,
		FollowOnlyTop:        followTop,
		FollowInteractionTop: mergedTop,
	}, nil
}

func cloneNodeSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func uniformTeleport(nodeSet map[string]struct{}) map[string]float64 {
	n := float64(len(nodeSet))
	out := make(map[string]float64, len(nodeSet))
	if n == 0 {
		return out
	}
	for node := range nodeSet {
		out[node] = 1.0 / n
	}
	return out
}

func topPubkeys(ranked []rankNode, topN int) []string {
	if topN > len(ranked) {
		topN = len(ranked)
	}
	out := make([]string, 0, topN)
	for i := 0; i < topN; i++ {
		out = append(out, ranked[i].Pubkey)
	}
	return out
}

func setOverlap(a, b []string) int {
	set := make(map[string]struct{}, len(a))
	for _, v := range a {
		set[v] = struct{}{}
	}
	count := 0
	for _, v := range b {
		if _, ok := set[v]; ok {
			count++
		}
	}
	return count
}

func spearmanRankCorrelation(a, b []rankNode) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	rankA := make(map[string]float64, len(a))
	for i, node := range a {
		rankA[node.Pubkey] = float64(i + 1)
	}
	rankB := make(map[string]float64, len(b))
	for i, node := range b {
		rankB[node.Pubkey] = float64(i + 1)
	}
	keys := make([]string, 0, len(rankA))
	for pubkey := range rankA {
		if _, ok := rankB[pubkey]; ok {
			keys = append(keys, pubkey)
		}
	}
	n := float64(len(keys))
	if n < 2 {
		return 0
	}
	var sumD2 float64
	for _, pubkey := range keys {
		d := rankA[pubkey] - rankB[pubkey]
		sumD2 += d * d
	}
	return 1.0 - (6.0*sumD2)/(n*(n*n-1))
}
