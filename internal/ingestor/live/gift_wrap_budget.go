package live

import (
	"sync"
	"time"
)

// giftWrapBudget is an in-memory hourly counter per recipient pubkey.
// It lives on the ingestor process: a restart clears it, which only
// lets one extra hour of wraps through.
type giftWrapBudget struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	counts map[string]giftWrapWindow
}

type giftWrapWindow struct {
	start time.Time
	n     int
}

func (b *giftWrapBudget) setLimit(perHour int) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if perHour < 0 {
		perHour = 0
	}
	b.limit = perHour
	if b.window <= 0 {
		b.window = time.Hour
	}
	if b.counts == nil {
		b.counts = make(map[string]giftWrapWindow)
	}
}

func (p *Processor) allowGiftWrap(recipient string) bool {
	if p == nil {
		return true
	}
	return p.giftWrap.allow(time.Now(), recipient)
}

func (b *giftWrapBudget) allow(now time.Time, recipient string) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limit <= 0 {
		return true
	}
	if b.window <= 0 {
		b.window = time.Hour
	}
	if b.counts == nil {
		b.counts = make(map[string]giftWrapWindow)
	}
	slot := b.counts[recipient]
	if slot.start.IsZero() || now.Sub(slot.start) >= b.window {
		slot = giftWrapWindow{start: now, n: 0}
	}
	if slot.n >= b.limit {
		b.counts[recipient] = slot
		b.prune(now)
		return false
	}
	slot.n++
	b.counts[recipient] = slot
	b.prune(now)
	return true
}

func (b *giftWrapBudget) prune(now time.Time) {
	if len(b.counts) < 4096 {
		return
	}
	for recipient, slot := range b.counts {
		if now.Sub(slot.start) >= b.window {
			delete(b.counts, recipient)
		}
	}
}
