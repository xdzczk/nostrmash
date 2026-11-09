package query

import (
	"math"
	"sort"
)

const (
	// PersonalizationSourcePersonalized is a ranking blended toward the
	// viewer's follows.
	PersonalizationSourcePersonalized = "personalized"
	// PersonalizationSourceGlobalFallback is the global ranking, used when
	// the viewer follows nobody.
	PersonalizationSourceGlobalFallback = "global_fallback"
)

// PersonalizationMeta describes how a discovery response was scoped to a viewer.
type PersonalizationMeta struct {
	Viewer       string
	Source       string
	FollowCount  int
	Truncated    bool
	CohortWeight float64
}

// blendRow is one candidate in a viewer-scoped ranking. CohortScore and
// GlobalScore are raw and are normalized across the candidate set before
// they are mixed. CohortEvidence breaks ties.
type blendRow[T any] struct {
	ID             string
	Item           T
	CohortScore    float64
	GlobalScore    float64
	CohortEvidence float64
}

// Blended is one row after mixing, in rank order for the requested page.
type Blended[T any] struct {
	Item           T
	Score          float64
	CohortEvidence float64
}

// blendRanked normalizes cohort and global scores by the max in the
// candidate set, mixes them with weight on the cohort, and pages the result.
// Sort is blended score, then cohort evidence, then id.
func blendRanked[T any](cohort, global []blendRow[T], weight float64, limit, offset int) []Blended[T] {
	if weight < 0 {
		weight = 0
	}
	if weight > 1 {
		weight = 1
	}
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	merged := mergeBlendRows(cohort, global)
	if len(merged) == 0 {
		return nil
	}
	maxCohort := 0.0
	maxGlobal := 0.0
	for _, row := range merged {
		if row.CohortScore > maxCohort {
			maxCohort = row.CohortScore
		}
		if row.GlobalScore > maxGlobal {
			maxGlobal = row.GlobalScore
		}
	}
	type scored struct {
		row   blendRow[T]
		score float64
	}
	ranked := make([]scored, 0, len(merged))
	for _, row := range merged {
		score := weight*normScore(row.CohortScore, maxCohort) + (1-weight)*normScore(row.GlobalScore, maxGlobal)
		ranked = append(ranked, scored{row: row, score: score})
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		if ranked[i].row.CohortEvidence != ranked[j].row.CohortEvidence {
			return ranked[i].row.CohortEvidence > ranked[j].row.CohortEvidence
		}
		return ranked[i].row.ID < ranked[j].row.ID
	})
	if offset >= len(ranked) {
		return []Blended[T]{}
	}
	end := offset + limit
	if end > len(ranked) {
		end = len(ranked)
	}
	out := make([]Blended[T], 0, end-offset)
	for _, row := range ranked[offset:end] {
		out = append(out, Blended[T]{
			Item:           row.row.Item,
			Score:          roundBlendScore(row.score),
			CohortEvidence: row.row.CohortEvidence,
		})
	}
	return out
}

func mergeBlendRows[T any](cohort, global []blendRow[T]) []blendRow[T] {
	byID := make(map[string]blendRow[T], len(cohort)+len(global))
	order := make([]string, 0, len(cohort)+len(global))
	addGlobal := func(row blendRow[T]) {
		if row.ID == "" {
			return
		}
		existing, ok := byID[row.ID]
		if !ok {
			byID[row.ID] = row
			order = append(order, row.ID)
			return
		}
		existing.GlobalScore = row.GlobalScore
		existing.Item = row.Item
		byID[row.ID] = existing
	}
	addCohort := func(row blendRow[T]) {
		if row.ID == "" {
			return
		}
		existing, ok := byID[row.ID]
		if !ok {
			byID[row.ID] = row
			order = append(order, row.ID)
			return
		}
		existing.CohortScore = row.CohortScore
		existing.CohortEvidence = row.CohortEvidence
		byID[row.ID] = existing
	}
	for _, row := range global {
		addGlobal(row)
	}
	for _, row := range cohort {
		addCohort(row)
	}
	out := make([]blendRow[T], 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out
}

func normScore(value, max float64) float64 {
	if max <= 0 || value <= 0 {
		return 0
	}
	return value / max
}

func roundBlendScore(value float64) float64 {
	if value <= 0 {
		return 0
	}
	return math.Round(value*1e6) / 1e6
}
