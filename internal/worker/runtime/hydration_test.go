package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/hydration"
	"github.com/xdzczk/nostrmash/internal/jobs"
)

func TestProcessHydrateAccountJob_NilService(t *testing.T) {
	err := processHydrateAccountJob(context.Background(), nil, jobs.Job{Payload: json.RawMessage(`{"pubkey":"abc"}`)})
	if err == nil || !strings.Contains(err.Error(), "hydration service unavailable") {
		t.Fatalf("expected unavailable error, got %v", err)
	}
}

func TestProcessHydrateAccountJob_BadPayload(t *testing.T) {
	// A zero-value service is enough: the decode failure returns before the
	// service is ever used.
	svc := &hydration.Service{}
	err := processHydrateAccountJob(context.Background(), svc, jobs.Job{Payload: json.RawMessage(`{not json`)})
	if err == nil || !strings.Contains(err.Error(), "decode hydrate payload") {
		t.Fatalf("expected decode error, got %v", err)
	}
}
