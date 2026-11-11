package query

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	cohortMemTTL     = 10 * time.Minute
	cohortMemEntries = 4096
)

// CohortCache is an optional shared cache for a viewer's follow list.
// Keys are the viewer pubkey. The adapter owns any key prefix.
type CohortCache interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
}

// ViewerCohort is the accounts whose engagement personalizes a ranking.
type ViewerCohort struct {
	Viewer    string   `json:"viewer"`
	Follows   []string `json:"follows"`
	Truncated bool     `json:"truncated"`
}

type cohortMemCache struct {
	mu    sync.Mutex
	items map[string]cohortMemEntry
}

type cohortMemEntry struct {
	cohort  ViewerCohort
	expires time.Time
}

func newPersonalizedState() (*cohortMemCache, *singleflight.Group) {
	return &cohortMemCache{items: map[string]cohortMemEntry{}}, &singleflight.Group{}
}

func (c *cohortMemCache) get(viewer string) (ViewerCohort, bool) {
	if c == nil {
		return ViewerCohort{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.items[viewer]
	if !ok {
		return ViewerCohort{}, false
	}
	if !entry.expires.After(time.Now()) {
		delete(c.items, viewer)
		return ViewerCohort{}, false
	}
	return entry.cohort, true
}

func (c *cohortMemCache) set(viewer string, cohort ViewerCohort) {
	if c == nil || viewer == "" {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.items {
		if !entry.expires.After(now) {
			delete(c.items, key)
		}
	}
	if _, exists := c.items[viewer]; !exists && len(c.items) >= cohortMemEntries {
		var oldest string
		var oldestAt time.Time
		for key, entry := range c.items {
			if oldest == "" || entry.expires.Before(oldestAt) {
				oldest = key
				oldestAt = entry.expires
			}
		}
		if oldest != "" {
			delete(c.items, oldest)
		}
	}
	c.items[viewer] = cohortMemEntry{cohort: cohort, expires: now.Add(cohortMemTTL)}
}

// PrepareViewerCohort loads and caches the viewer's follow list so a
// homepage fan-out does not resolve it once per section.
func (s Service) PrepareViewerCohort(ctx context.Context, viewer string) error {
	_, err := s.resolveViewerCohort(ctx, viewer)
	return err
}

func (s Service) resolveViewerCohort(ctx context.Context, viewer string) (ViewerCohort, error) {
	viewer = strings.TrimSpace(viewer)
	if viewer == "" {
		return ViewerCohort{}, nil
	}
	if cached, ok := s.cohortMem.get(viewer); ok {
		return cached, nil
	}
	if s.cohortFlight == nil {
		return s.loadViewerCohort(ctx, viewer)
	}
	loaded, err, _ := s.cohortFlight.Do(viewer, func() (any, error) {
		if cached, ok := s.cohortMem.get(viewer); ok {
			return cached, nil
		}
		return s.loadViewerCohort(ctx, viewer)
	})
	if err != nil {
		return ViewerCohort{}, err
	}
	return loaded.(ViewerCohort), nil
}

func (s Service) loadViewerCohort(ctx context.Context, viewer string) (ViewerCohort, error) {
	if s.cohortRedis != nil {
		if raw, ok, err := s.cohortRedis.Get(ctx, viewer); err == nil && ok {
			var cohort ViewerCohort
			if json.Unmarshal([]byte(raw), &cohort) == nil && cohort.Viewer == viewer {
				s.cohortMem.set(viewer, cohort)
				return cohort, nil
			}
		}
	}
	reader := s.capabilities.curated.followedPubkeys
	if reader == nil {
		return ViewerCohort{}, unsupportedCapabilityError("viewer follow list")
	}
	limit := s.personalizedMaxFollows
	if limit <= 0 {
		limit = 2000
	}
	follows, err := reader.ListFollowedPubkeys(ctx, viewer, limit+1)
	if err != nil {
		return ViewerCohort{}, err
	}
	truncated := len(follows) > limit
	if truncated {
		follows = follows[:limit]
	}
	cohort := ViewerCohort{Viewer: viewer, Follows: follows, Truncated: truncated}
	s.cohortMem.set(viewer, cohort)
	if s.cohortRedis != nil {
		if payload, err := json.Marshal(cohort); err == nil {
			_ = s.cohortRedis.Set(ctx, viewer, string(payload), cohortMemTTL)
		}
	}
	return cohort, nil
}

func (s Service) personalizationMeta(cohort ViewerCohort, source string) PersonalizationMeta {
	return PersonalizationMeta{
		Viewer:       cohort.Viewer,
		Source:       source,
		FollowCount:  len(cohort.Follows),
		Truncated:    cohort.Truncated,
		CohortWeight: s.personalizedCohortWeight,
	}
}

func (s Service) personalizedPage(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	if offset > 500 {
		offset = 500
	}
	return limit, offset
}

func (s Service) personalizedCandidateLimit() int {
	limit := s.personalizedCandidates
	if limit <= 0 {
		limit = 300
	}
	if limit > 500 {
		limit = 500
	}
	return limit
}
