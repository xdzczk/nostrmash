package meili

import (
	"testing"
	"time"
)

func TestSearchCircuit_OpensAfterThreshold(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	c := &searchCircuit{
		failureThreshold: 3,
		openFor:          30 * time.Second,
		now:              func() time.Time { return now },
	}
	for i := 0; i < 2; i++ {
		c.failure()
		if !c.allow() {
			t.Fatalf("circuit should stay closed after %d failures", i+1)
		}
	}
	c.failure()
	if c.allow() {
		t.Fatal("circuit should be open after threshold failures")
	}
	now = now.Add(31 * time.Second)
	if !c.allow() {
		t.Fatal("circuit should half-open after openFor elapses")
	}
	c.success()
	if c.open() {
		t.Fatal("circuit should be closed after success")
	}
}
