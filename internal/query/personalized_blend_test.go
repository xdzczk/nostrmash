package query

import "testing"

func TestBlendRanked_CohortDominatesGlobalOnly(t *testing.T) {
	cohort := []blendRow[string]{{ID: "cohort", Item: "cohort", CohortScore: 10, CohortEvidence: 2}}
	global := []blendRow[string]{{ID: "global", Item: "global", GlobalScore: 1000}}
	got := blendRanked(cohort, global, 0.85, 10, 0)
	if len(got) != 2 {
		t.Fatalf("len = %d", len(got))
	}
	if got[0].Item != "cohort" || got[0].Score != 0.85 {
		t.Fatalf("first = %+v", got[0])
	}
	if got[1].Item != "global" || got[1].Score != 0.15 {
		t.Fatalf("second = %+v", got[1])
	}
}

func TestBlendRanked_BothSidesOutrankCohortOnly(t *testing.T) {
	cohort := []blendRow[string]{
		{ID: "only", Item: "only", CohortScore: 10, CohortEvidence: 1},
		{ID: "both", Item: "cohort-both", CohortScore: 10, CohortEvidence: 4},
	}
	global := []blendRow[string]{
		{ID: "both", Item: "global-both", GlobalScore: 50},
	}
	got := blendRanked(cohort, global, 0.85, 10, 0)
	if len(got) != 2 || got[0].Item != "global-both" {
		t.Fatalf("order = %+v", got)
	}
	if got[0].Score != 1 {
		t.Fatalf("blended score = %v", got[0].Score)
	}
	if got[0].CohortEvidence != 4 {
		t.Fatalf("evidence = %v", got[0].CohortEvidence)
	}
}

func TestBlendRanked_TieBreaksByEvidenceThenID(t *testing.T) {
	cohort := []blendRow[string]{
		{ID: "b", Item: "b", CohortScore: 5, CohortEvidence: 1},
		{ID: "a", Item: "a", CohortScore: 5, CohortEvidence: 1},
		{ID: "c", Item: "c", CohortScore: 5, CohortEvidence: 3},
	}
	got := blendRanked(cohort, nil, 0.85, 10, 0)
	ids := []string{got[0].Item, got[1].Item, got[2].Item}
	if ids[0] != "c" || ids[1] != "a" || ids[2] != "b" {
		t.Fatalf("order = %v", ids)
	}
}

func TestBlendRanked_EmptyCohortFollowsGlobal(t *testing.T) {
	global := []blendRow[string]{
		{ID: "low", Item: "low", GlobalScore: 1},
		{ID: "high", Item: "high", GlobalScore: 9},
	}
	got := blendRanked(nil, global, 0.85, 10, 0)
	if len(got) != 2 || got[0].Item != "high" || got[1].Item != "low" {
		t.Fatalf("order = %+v", got)
	}
}

func TestBlendRanked_Paginates(t *testing.T) {
	cohort := []blendRow[string]{
		{ID: "a", Item: "a", CohortScore: 3},
		{ID: "b", Item: "b", CohortScore: 2},
		{ID: "c", Item: "c", CohortScore: 1},
	}
	got := blendRanked(cohort, nil, 0.85, 1, 1)
	if len(got) != 1 || got[0].Item != "b" {
		t.Fatalf("page = %+v", got)
	}
	if past := blendRanked(cohort, nil, 0.85, 10, 5); len(past) != 0 {
		t.Fatalf("past end = %+v", past)
	}
}
