package relay

import "time"

type boundedBackoff struct {
	initial time.Duration
	max     time.Duration
	current time.Duration
}

func newBoundedBackoff(initial, max time.Duration) *boundedBackoff {
	return &boundedBackoff{
		initial: initial,
		max:     max,
	}
}

func (b *boundedBackoff) Next() time.Duration {
	if b.current == 0 {
		b.current = b.initial
		return b.current
	}
	next := b.current * 2
	if next > b.max {
		next = b.max
	}
	b.current = next
	return b.current
}

func (b *boundedBackoff) Reset() {
	b.current = 0
}
