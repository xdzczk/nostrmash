package query

import (
	"context"
	"testing"
	"time"
)

type followedReader struct {
	fakeReader
	follows []string
	calls   int
}

func (f *followedReader) ListFollowedPubkeys(_ context.Context, _ string, limit int) ([]string, error) {
	f.calls++
	if limit > 0 && len(f.follows) > limit {
		return append([]string(nil), f.follows[:limit]...), nil
	}
	return append([]string(nil), f.follows...), nil
}

func TestResolveViewerCohort_TruncatesAndCaches(t *testing.T) {
	reader := &followedReader{follows: []string{"rank1", "rank2", "rank3"}}
	maxFollows := 2
	svc := mustNewServiceWithOptions(t, reader, ServiceOptions{PersonalizedMaxFollows: maxFollows})
	first, err := svc.resolveViewerCohort(context.Background(), "viewer")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !first.Truncated || len(first.Follows) != 2 || first.Follows[0] != "rank1" || first.Follows[1] != "rank2" {
		t.Fatalf("cohort = %+v", first)
	}
	if _, err := svc.resolveViewerCohort(context.Background(), "viewer"); err != nil {
		t.Fatalf("cached resolve: %v", err)
	}
	if reader.calls != 1 {
		t.Fatalf("list calls = %d, want 1", reader.calls)
	}
}

func TestResolveViewerCohort_ZeroFollows(t *testing.T) {
	reader := &followedReader{}
	svc := mustNewServiceWithOptions(t, reader, ServiceOptions{})
	cohort, err := svc.resolveViewerCohort(context.Background(), "viewer")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if cohort.Truncated || len(cohort.Follows) != 0 || cohort.Viewer != "viewer" {
		t.Fatalf("cohort = %+v", cohort)
	}
}

func TestResolveViewerCohort_RedisHitSkipsStore(t *testing.T) {
	cache := &memoryStringCache{values: map[string]string{
		"viewer": `{"viewer":"viewer","follows":["a"],"truncated":false}`,
	}}
	reader := &followedReader{follows: []string{"should-not-load"}}
	svc := mustNewServiceWithOptions(t, reader, ServiceOptions{CohortCache: cache})
	cohort, err := svc.resolveViewerCohort(context.Background(), "viewer")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(cohort.Follows) != 1 || cohort.Follows[0] != "a" {
		t.Fatalf("cohort = %+v", cohort)
	}
	if reader.calls != 0 {
		t.Fatalf("store was queried")
	}
}

type memoryStringCache struct {
	values map[string]string
}

func (c *memoryStringCache) Get(_ context.Context, key string) (string, bool, error) {
	raw, ok := c.values[key]
	return raw, ok, nil
}

func (c *memoryStringCache) Set(_ context.Context, key string, value string, _ time.Duration) error {
	c.values[key] = value
	return nil
}
