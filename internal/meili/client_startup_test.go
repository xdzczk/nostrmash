package meili

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewClient_DoesNotRequireReachableMeilisearch(t *testing.T) {
	client, err := NewClient(Config{
		Enabled: true,
		URL:     "http://127.0.0.1:1",
	})
	if err != nil {
		t.Fatalf("NewClient should not ping Meilisearch: %v", err)
	}
	if !client.Enabled() {
		t.Fatal("expected enabled client even when the host is unreachable")
	}
}

func TestRetryUntil_SucceedsAfterTransientFailures(t *testing.T) {
	attempts := 0
	err := retryUntil(context.Background(), time.Second, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("connection refused")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
}

func TestRetryUntil_RespectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := retryUntil(ctx, time.Second, func(context.Context) error {
		return errors.New("connection refused")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRetryUntil_TimesOut(t *testing.T) {
	err := retryUntil(context.Background(), 50*time.Millisecond, func(context.Context) error {
		return errors.New("connection refused")
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("expected wrapped last error, got %v", err)
	}
}
