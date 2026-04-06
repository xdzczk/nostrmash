package derivation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestDecodeEventJobPayload(t *testing.T) {
	t.Run("valid payload trims event id", func(t *testing.T) {
		payload, err := decodeEventJobPayload(json.RawMessage(`{"event_id":"  abc  "}`))
		if err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.EventID != "abc" {
			t.Fatalf("unexpected event_id: got %q want %q", payload.EventID, "abc")
		}
	})

	t.Run("rejects missing event id", func(t *testing.T) {
		_, err := decodeEventJobPayload(json.RawMessage(`{"event_id":"   "}`))
		if err == nil || !strings.Contains(err.Error(), "event_id is required") {
			t.Fatalf("expected required event_id error, got %v", err)
		}
	})
}

func TestProcessJobValidation(t *testing.T) {
	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := ProcessJob(ctx, &Handlers{}, Job{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
	})

	t.Run("nil handlers", func(t *testing.T) {
		err := ProcessJob(context.Background(), nil, Job{JobType: JobTypeDeriveEventBundle})
		if err == nil || !strings.Contains(err.Error(), "handlers are not configured") {
			t.Fatalf("expected handlers not configured error, got %v", err)
		}
	})

	t.Run("unknown job type", func(t *testing.T) {
		err := ProcessJob(context.Background(), &Handlers{}, Job{JobType: "unknown_job"})
		if err == nil || !strings.Contains(err.Error(), "not implemented") {
			t.Fatalf("expected not implemented error, got %v", err)
		}
	})

	t.Run("invalid event payload", func(t *testing.T) {
		err := ProcessJob(context.Background(), &Handlers{}, Job{
			JobType: JobTypeDeriveEventBundle,
			Payload: json.RawMessage(`{`),
		})
		if err == nil || !strings.Contains(err.Error(), "decode job payload") {
			t.Fatalf("expected decode payload error, got %v", err)
		}
	})

	t.Run("missing event id in payload", func(t *testing.T) {
		err := ProcessJob(context.Background(), &Handlers{}, Job{
			JobType: JobTypeDeriveEventBundle,
			Payload: json.RawMessage(`{"event_id":"   "}`),
		})
		if err == nil || !strings.Contains(err.Error(), "event_id is required") {
			t.Fatalf("expected required event_id error, got %v", err)
		}
	})

	t.Run("rebuild payload requires run id", func(t *testing.T) {
		err := ProcessJob(context.Background(), &Handlers{}, Job{
			JobType: JobTypeRebuildProjectionScope,
			Payload: json.RawMessage(`{"run_id":0}`),
		})
		if err == nil || !strings.Contains(err.Error(), "run_id is required") {
			t.Fatalf("expected required run_id error, got %v", err)
		}
	})
}

func TestProcessJobDispatchesToHandlerMethods(t *testing.T) {
	tests := []struct {
		name    string
		jobType string
		payload json.RawMessage
	}{
		{
			name:    "derive relationships",
			jobType: JobTypeDeriveEventRelationships,
			payload: json.RawMessage(`{"event_id":"evt1"}`),
		},
		{
			name:    "project profiles latest",
			jobType: JobTypeProjectProfilesLatest,
			payload: json.RawMessage(`{"event_id":"evt2"}`),
		},
		{
			name:    "execute rebuild run",
			jobType: JobTypeRebuildProjectionScope,
			payload: json.RawMessage(`{"run_id":1}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ProcessJob(context.Background(), &Handlers{}, Job{
				JobType: tc.jobType,
				Payload: tc.payload,
			})
			if err == nil || !strings.Contains(err.Error(), "handlers are not initialized") {
				t.Fatalf("expected handlers are not initialized error, got %v", err)
			}
		})
	}
}
