package derivation

import (
	"sync"
	"time"
)

// reconcileCooldown records pubkeys that should not be full-recomputed again
// until a deadline. It exists so one account that times out does not consume
// a statement_timeout on every reconciliation pass.
type reconcileCooldown struct {
	mu    sync.Mutex
	until map[string]time.Time
}

func newReconcileCooldown() *reconcileCooldown {
	return &reconcileCooldown{until: map[string]time.Time{}}
}

func (c *reconcileCooldown) skip(pubkey string, now time.Time) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	until, ok := c.until[pubkey]
	if !ok {
		return false
	}
	if !until.After(now) {
		delete(c.until, pubkey)
		return false
	}
	return true
}

func (c *reconcileCooldown) mark(pubkey string, until time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.until == nil {
		c.until = map[string]time.Time{}
	}
	c.until[pubkey] = until
}
