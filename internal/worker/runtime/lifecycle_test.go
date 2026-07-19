package runtime

import (
	"sync"
	"testing"
	"time"
)

func TestWaitWithTimeout_CompletesBeforeDeadline(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		wg.Done()
	}()

	timedOut := false
	waitWithTimeout(&wg, time.Second, func() { timedOut = true })
	if timedOut {
		t.Fatal("waitWithTimeout reported timeout for a loop that exited in time")
	}
}

func TestWaitWithTimeout_FiresOnTimeout(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	release := make(chan struct{})
	go func() {
		<-release
		wg.Done()
	}()
	defer close(release)

	timedOut := false
	start := time.Now()
	waitWithTimeout(&wg, 30*time.Millisecond, func() { timedOut = true })
	if !timedOut {
		t.Fatal("expected timeout callback to fire for a loop that never exits")
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("returned before timeout elapsed: %s", elapsed)
	}
}
