package store

import (
	"context"
	"encoding/json"
	"testing"
)

func TestAttachTopZapsNilReadDoesNotPanic(t *testing.T) {
	s := &PostgresStore{}
	events := []json.RawMessage{json.RawMessage(`{"id":"abc"}`)}
	out := s.AttachTopZaps(context.Background(), events)
	if len(out) != 1 || string(out[0]) != `{"id":"abc"}` {
		t.Fatalf("events = %#v", out)
	}
	if _, err := s.TopZapAdsByEventIDs(context.Background(), []string{"abc"}); err == nil {
		t.Fatal("expected uninitialized store error")
	}
}
