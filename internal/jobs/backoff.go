package jobs

import (
	"math/rand/v2"
	"time"
)

// DefaultMaxRetryDelay caps exponential retry growth. With the default five
// max attempts and a 5s base, delays run 5s, 10s, 20s, 40s — the cap only
// bites for configurations with more attempts or larger bases.
const DefaultMaxRetryDelay = 5 * time.Minute

// retryJitterFraction spreads retries of jobs that failed together (e.g. a
// database blip failing a whole claimed batch) so they do not all become
// runnable in the same poll tick. ±20% of the computed delay.
const retryJitterFraction = 0.2

// RetryDelay returns how long a job that has already failed `attempts` times
// should wait before its next run: base * 2^attempts, capped at max, with
// ±20% jitter. A fixed delay (the previous behavior) made systemic failures
// thundering herds — every failed job retried in lockstep at exactly the same
// cadence regardless of how many times it had already failed.
func RetryDelay(attempts int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = 5 * time.Second
	}
	if max <= 0 {
		max = DefaultMaxRetryDelay
	}
	if max < base {
		max = base
	}
	delay := base
	for i := 0; i < attempts; i++ {
		delay *= 2
		if delay >= max || delay <= 0 { // <= 0 guards duration overflow
			delay = max
			break
		}
	}
	jitter := time.Duration((rand.Float64()*2 - 1) * retryJitterFraction * float64(delay))
	delay += jitter
	if delay < base {
		delay = base
	}
	return delay
}
