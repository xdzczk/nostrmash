package api_primal

import (
	"testing"
)

func TestSendFrameQueuesWithoutWriting(t *testing.T) {
	s := &wsConnSession{
		done:       make(chan struct{}),
		writeQueue: make(chan []byte, 2),
	}
	if err := s.sendFrame([]any{"EOSE", "sub1"}); err != nil {
		t.Fatalf("sendFrame: %v", err)
	}
	if got := len(s.writeQueue); got != 1 {
		t.Fatalf("expected 1 queued frame, got %d", got)
	}
}

func TestSendFrameFailsAfterSessionClosed(t *testing.T) {
	s := &wsConnSession{
		done:       make(chan struct{}),
		writeQueue: make(chan []byte, 1),
	}
	close(s.done)
	if err := s.sendFrame([]any{"NOTICE", "", "x"}); err != errSessionClosed {
		t.Fatalf("expected errSessionClosed, got %v", err)
	}
}

func TestSendFrameBackpressureDisconnects(t *testing.T) {
	prev := wsWriteQueueWait
	wsWriteQueueWait = 0
	t.Cleanup(func() { wsWriteQueueWait = prev })

	s := &wsConnSession{
		done:       make(chan struct{}),
		writeQueue: make(chan []byte, 1),
	}
	s.writeQueue <- []byte(`["NOTICE","",""]`)
	if err := s.sendFrame([]any{"EOSE", "sub"}); err != errWriteBackpressure {
		t.Fatalf("expected errWriteBackpressure, got %v", err)
	}
}
