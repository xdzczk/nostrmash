package jobs

import (
	"testing"
	"time"
)

func TestRetryDelayGrowsExponentiallyWithinJitterBounds(t *testing.T) {
	base := 5 * time.Second
	max := 5 * time.Minute
	for attempts := 0; attempts <= 4; attempts++ {
		expected := base << attempts
		lower := time.Duration(float64(expected) * (1 - retryJitterFraction))
		if lower < base {
			lower = base
		}
		upper := time.Duration(float64(expected) * (1 + retryJitterFraction))
		for i := 0; i < 50; i++ {
			got := RetryDelay(attempts, base, max)
			if got < lower || got > upper {
				t.Fatalf("attempts=%d: delay %v outside [%v, %v]", attempts, got, lower, upper)
			}
		}
	}
}

func TestRetryDelayIsCapped(t *testing.T) {
	base := 5 * time.Second
	max := 30 * time.Second
	upper := time.Duration(float64(max) * (1 + retryJitterFraction))
	for i := 0; i < 50; i++ {
		if got := RetryDelay(10, base, max); got > upper {
			t.Fatalf("capped delay %v exceeds %v", got, upper)
		}
	}
	// Extreme attempt counts must not overflow into negative durations.
	if got := RetryDelay(1000, base, max); got <= 0 || got > upper {
		t.Fatalf("overflow guard failed: %v", got)
	}
}

func TestRetryDelayNeverBelowBase(t *testing.T) {
	base := 5 * time.Second
	for i := 0; i < 100; i++ {
		if got := RetryDelay(0, base, DefaultMaxRetryDelay); got < base {
			t.Fatalf("delay %v below base %v", got, base)
		}
	}
}

func TestRetryDelayDefaultsForNonPositiveInputs(t *testing.T) {
	if got := RetryDelay(0, 0, 0); got <= 0 {
		t.Fatalf("expected positive default delay, got %v", got)
	}
	// max below base is clamped up to base rather than truncating growth.
	got := RetryDelay(3, 10*time.Second, time.Second)
	if got < 8*time.Second {
		t.Fatalf("expected max clamped to base, got %v", got)
	}
}
