package failure

import (
	"context"
	"errors"
	"testing"
)

func TestClassifyError_AdditionalBranches(t *testing.T) {
	if got := ClassifyError(nil); got.Class != ClassUnknown || got.Reason != "none" {
		t.Fatalf("nil = %+v", got)
	}
	if got := ClassifyError(context.Canceled); got.Class != ClassDependencyTransient || got.Reason != "context_canceled" {
		t.Fatalf("canceled = %+v", got)
	}
	if got := ClassifyError(errors.New("queue claim failed")); got.Class != ClassQueueJob {
		t.Fatalf("queue = %+v", got)
	}
	if got := ClassifyError(errors.New("database unavailable")); got.Class != ClassStorage {
		t.Fatalf("database = %+v", got)
	}
	if got := ClassifyError(errors.New("unexpected nil pointer")); got.Class != ClassInternalBug {
		t.Fatalf("default = %+v", got)
	}
	if got := FromPanic(123).(PanicError).Error(); got != "panic: 123" {
		t.Fatalf("PanicError.Error = %q", got)
	}
}

func TestClassifyHTTP_AdditionalBranches(t *testing.T) {
	if got := ClassifyHTTP(503, "dependency_unavailable"); got.Class != ClassDependencyTransient {
		t.Fatalf("503 = %+v", got)
	}
	if got := ClassifyHTTP(500, "boom"); got.Class != ClassInternalBug {
		t.Fatalf("500 = %+v", got)
	}
	if got := ClassifyHTTP(200, "ok"); got.Class != ClassUnknown {
		t.Fatalf("200 = %+v", got)
	}
}
