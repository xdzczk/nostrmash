package relayprobe

import (
	"testing"
)

func TestClassifyFrame_Event(t *testing.T) {
	frame := []byte(`["EVENT","sub-1",{"id":"abc","kind":0}]`)
	if got := classifyFrame(frame); got != "EVENT" {
		t.Fatalf("expected EVENT, got %s", got)
	}
}

func TestClassifyFrame_EOSE(t *testing.T) {
	frame := []byte(`["EOSE","sub-1"]`)
	if got := classifyFrame(frame); got != "EOSE" {
		t.Fatalf("expected EOSE, got %s", got)
	}
}

func TestClassifyFrame_Notice(t *testing.T) {
	frame := []byte(`["NOTICE","rate limited"]`)
	if got := classifyFrame(frame); got != "NOTICE" {
		t.Fatalf("expected NOTICE, got %s", got)
	}
}

func TestClassifyFrame_Closed(t *testing.T) {
	frame := []byte(`["CLOSED","sub-1","auth-required:"]`)
	if got := classifyFrame(frame); got != "CLOSED" {
		t.Fatalf("expected CLOSED, got %s", got)
	}
}

func TestClassifyFrame_Invalid(t *testing.T) {
	if got := classifyFrame([]byte("not json")); got != "unknown" {
		t.Fatalf("expected unknown for invalid json, got %s", got)
	}
	if got := classifyFrame([]byte("[]")); got != "unknown" {
		t.Fatalf("expected unknown for empty array, got %s", got)
	}
	if got := classifyFrame(nil); got != "unknown" {
		t.Fatalf("expected unknown for nil, got %s", got)
	}
}

func TestTruncateError(t *testing.T) {
	short := "short error"
	if got := truncateError(errMsg(short)); got != short {
		t.Fatalf("short error should not be truncated, got %s", got)
	}

	long := make([]byte, 300)
	for i := range long {
		long[i] = 'x'
	}
	got := truncateError(errMsg(string(long)))
	if len(got) != 200 {
		t.Fatalf("expected truncated to 200 chars, got %d", len(got))
	}
}

type errMsg string

func (e errMsg) Error() string { return string(e) }
