package meili

import (
	"sync"
	"time"
)

// searchCircuit trips open after consecutive Meilisearch search failures so
// request handlers fall through to Postgres quickly instead of waiting on a
// thrashing search node.
type searchCircuit struct {
	mu               sync.Mutex
	failures         int
	openUntil        time.Time
	failureThreshold int
	openFor          time.Duration
	now              func() time.Time
}

func newSearchCircuit() *searchCircuit {
	return &searchCircuit{
		failureThreshold: 5,
		openFor:          30 * time.Second,
		now:              time.Now,
	}
}

func (c *searchCircuit) allow() bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.openUntil.IsZero() {
		return true
	}
	if c.now().Before(c.openUntil) {
		return false
	}
	// Half-open: allow a single probe.
	c.openUntil = time.Time{}
	c.failures = 0
	return true
}

func (c *searchCircuit) success() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.openUntil = time.Time{}
}

func (c *searchCircuit) failure() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures++
	if c.failures >= c.failureThreshold {
		c.openUntil = c.now().Add(c.openFor)
		c.failures = 0
	}
}

func (c *searchCircuit) open() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.openUntil.IsZero() && c.now().Before(c.openUntil)
}
