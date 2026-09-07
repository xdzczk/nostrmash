package api

import (
	"fmt"
	"testing"
	"time"
)

func TestRateLimiterEvictsIdleBuckets(t *testing.T) {
	limiter := newRateLimiter(HTTPRateLimitOptions{DefaultRPM: 60, DefaultBurst: 10})
	start := time.Now()

	for i := 0; i < 100; i++ {
		if !limiter.allow(fmt.Sprintf("ip-%d:default", i), 60, 10, start) {
			t.Fatalf("first request for client %d should be allowed", i)
		}
	}
	if got := len(limiter.buckets); got != 100 {
		t.Fatalf("expected 100 buckets, got %d", got)
	}

	// One active client keeps requesting; idle ones must be swept once both
	// the sweep interval and the idle-eviction window have elapsed.
	activeAt := start.Add(bucketIdleEviction + time.Second)
	if !limiter.allow("ip-0:default", 60, 10, activeAt) {
		t.Fatal("active client should be allowed after refill window")
	}
	if got := len(limiter.buckets); got != 1 {
		t.Fatalf("expected only the active bucket to survive, got %d", got)
	}
	if _, ok := limiter.buckets["ip-0:default"]; !ok {
		t.Fatal("active bucket must not be evicted")
	}
}

func TestRateLimiterSweepIsAmortized(t *testing.T) {
	limiter := newRateLimiter(HTTPRateLimitOptions{DefaultRPM: 60, DefaultBurst: 10})
	start := time.Now()

	limiter.allow("ip-a:default", 60, 10, start)
	limiter.allow("ip-b:default", 60, 10, start)

	// Just past the idle window but within the sweep interval since the last
	// sweep ran: nothing is scanned yet.
	limiter.lastSweep = start.Add(bucketIdleEviction)
	limiter.allow("ip-a:default", 60, 10, start.Add(bucketIdleEviction+time.Second))
	if got := len(limiter.buckets); got != 2 {
		t.Fatalf("sweep should not run before interval elapses, got %d buckets", got)
	}

	// Once the interval has passed, the idle bucket goes away.
	limiter.allow("ip-a:default", 60, 10, start.Add(bucketIdleEviction+bucketSweepInterval+2*time.Second))
	if _, ok := limiter.buckets["ip-b:default"]; ok {
		t.Fatal("idle bucket should be evicted after sweep interval")
	}
}

func TestRateLimiterEvictionIsLossless(t *testing.T) {
	limiter := newRateLimiter(HTTPRateLimitOptions{DefaultRPM: 60, DefaultBurst: 2})
	start := time.Now()

	// Drain the bucket.
	limiter.allow("ip-a:default", 60, 2, start)
	limiter.allow("ip-a:default", 60, 2, start)
	if limiter.allow("ip-a:default", 60, 2, start) {
		t.Fatal("third request in same instant should be limited")
	}

	// After the idle-eviction window a fresh bucket grants a full burst — the
	// same tokens a surviving bucket would have refilled by then.
	later := start.Add(bucketIdleEviction + bucketSweepInterval)
	if !limiter.allow("ip-a:default", 60, 2, later) {
		t.Fatal("request after full refill window should be allowed")
	}
	if !limiter.allow("ip-a:default", 60, 2, later) {
		t.Fatal("burst capacity should be fully restored")
	}
	if limiter.allow("ip-a:default", 60, 2, later) {
		t.Fatal("burst should be exhausted again")
	}
}
