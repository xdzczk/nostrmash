package query

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type cohortDiscoveryReader struct {
	fakeReader
	follows     []string
	listCalls   int
	globalLimit int
	cohortCalls int
	cohortNotes []readmodel.CohortNote
	globalNotes []readmodel.TrendingNote
}

func (r *cohortDiscoveryReader) ListFollowedPubkeys(context.Context, string, int) ([]string, error) {
	r.listCalls++
	return append([]string(nil), r.follows...), nil
}

func (r *cohortDiscoveryReader) GetTrendingNotes(_ context.Context, _ time.Duration, _ string, limit, _ int) ([]readmodel.TrendingNote, error) {
	r.globalLimit = limit
	return append([]readmodel.TrendingNote(nil), r.globalNotes...), nil
}

func (r *cohortDiscoveryReader) GetCohortTrendingNotes(context.Context, []string, []int, time.Duration, string, int) ([]readmodel.CohortNote, error) {
	r.cohortCalls++
	return append([]readmodel.CohortNote(nil), r.cohortNotes...), nil
}

func (r *cohortDiscoveryReader) GetCohortHotConversations(context.Context, []string, time.Duration, string, int) ([]readmodel.CohortConversation, error) {
	return nil, nil
}
func (r *cohortDiscoveryReader) GetCohortZappedNotes(context.Context, []string, time.Duration, string, int) ([]readmodel.CohortNote, error) {
	return nil, nil
}
func (r *cohortDiscoveryReader) GetCohortBookmarkedNotes(context.Context, []string, string, int) ([]readmodel.CohortNote, error) {
	return nil, nil
}
func (r *cohortDiscoveryReader) GetCohortFollowedProfiles(context.Context, string, []string, int) ([]readmodel.CohortProfile, error) {
	return nil, nil
}
func (r *cohortDiscoveryReader) GetCohortSupportedProfiles(context.Context, []string, time.Duration, int64, int) ([]readmodel.CohortProfile, error) {
	return nil, nil
}
func (r *cohortDiscoveryReader) GetCohortTrendingProfiles(context.Context, []string, time.Duration, string, int) ([]readmodel.CohortProfile, error) {
	return nil, nil
}
func (r *cohortDiscoveryReader) GetCohortRisingProfiles(context.Context, string, []string, time.Duration, string, int) ([]readmodel.CohortProfile, error) {
	return nil, nil
}
func (r *cohortDiscoveryReader) GetCohortTrendingHashtags(context.Context, []string, time.Duration, string, int) ([]readmodel.TrendingHashtag, error) {
	return nil, nil
}
func (r *cohortDiscoveryReader) GetCohortTrendingDomains(context.Context, []string, time.Duration, string, int) ([]readmodel.CohortDomain, error) {
	return nil, nil
}

func TestGetPersonalizedTrendingNotes_BlendsCohortAheadOfGlobal(t *testing.T) {
	reader := &cohortDiscoveryReader{
		follows: []string{"followed"},
		cohortNotes: []readmodel.CohortNote{{
			Note:        readmodel.TrendingNote{EventID: "cohort-note", Score: 1},
			CohortScore: 80,
		}},
		globalNotes: []readmodel.TrendingNote{
			{EventID: "global-note", Score: 100},
			{EventID: "cohort-note", Score: 1},
		},
	}
	svc := mustNewServiceWithOptions(t, reader, ServiceOptions{PersonalizedCohortWeight: 0.85, PersonalizedCandidates: 300})
	rows, meta, err := svc.GetPersonalizedTrendingNotes(context.Background(), 24*time.Hour, DiscoveryScope{Viewer: "viewer"}, 20, 0)
	if err != nil {
		t.Fatalf("personalized notes: %v", err)
	}
	if meta.Source != PersonalizationSourcePersonalized || meta.FollowCount != 1 || meta.CohortWeight != 0.85 {
		t.Fatalf("meta = %+v", meta)
	}
	if len(rows) != 2 || rows[0].EventID != "cohort-note" {
		t.Fatalf("rows = %+v", rows)
	}
	if rows[0].Score <= rows[1].Score {
		t.Fatalf("blended scores = %v then %v", rows[0].Score, rows[1].Score)
	}
	if rows[0].CohortEvidence != 80 {
		t.Fatalf("cohort evidence = %v", rows[0].CohortEvidence)
	}
	if reader.globalLimit != 300 {
		t.Fatalf("global candidate limit = %d", reader.globalLimit)
	}
}

func TestGetPersonalizedTrendingNotes_ZeroFollowsUsesGlobalPage(t *testing.T) {
	reader := &cohortDiscoveryReader{
		globalNotes: []readmodel.TrendingNote{{EventID: "global-note", Score: 4}},
	}
	svc := mustNewServiceWithOptions(t, reader, ServiceOptions{})
	rows, meta, err := svc.GetPersonalizedTrendingNotes(context.Background(), 24*time.Hour, DiscoveryScope{Viewer: "viewer"}, 20, 0)
	if err != nil {
		t.Fatalf("personalized notes: %v", err)
	}
	if meta.Source != PersonalizationSourceGlobalFallback || meta.FollowCount != 0 {
		t.Fatalf("meta = %+v", meta)
	}
	if len(rows) != 1 || rows[0].EventID != "global-note" || rows[0].Score != 4 {
		t.Fatalf("rows = %+v", rows)
	}
	if reader.globalLimit != 20 || reader.cohortCalls != 0 {
		t.Fatalf("global limit %d cohort calls %d", reader.globalLimit, reader.cohortCalls)
	}
}
