package jobs

import (
	"context"
	"testing"
	"time"
)

// Timeout is a local timer, not "the shared LISTEN channel stayed quiet".
// CI runs packages in parallel against one Postgres, so other Enqueue calls
// fire pg_notify on nostrmash_jobs and would make a DB-backed timeout test
// return immediately (main CI after #35).
func TestNotifyListenerWaitTimesOutWithoutBroadcast(t *testing.T) {
	l := &notifyListener{gen: make(chan struct{})}
	start := time.Now()
	l.Wait(context.Background(), 80*time.Millisecond)
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Fatalf("Wait returned too quickly without a broadcast: %v", elapsed)
	}
}

func TestNotifyListenerWaitReturnsOnBroadcast(t *testing.T) {
	l := &notifyListener{gen: make(chan struct{})}
	go func() {
		time.Sleep(10 * time.Millisecond)
		l.broadcast()
	}()
	start := time.Now()
	l.Wait(context.Background(), 2*time.Second)
	if elapsed := time.Since(start); elapsed > 750*time.Millisecond {
		t.Fatalf("Wait ignored broadcast and sat on the timer: %v", elapsed)
	}
}
