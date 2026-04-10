package runtime

import (
	"context"
	"testing"

	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
)

func TestFetchAuthorMetadata_ReturnsNilOnEmptyRelays(t *testing.T) {
	t.Parallel()
	err := fetchAuthorMetadata(
		context.Background(),
		fakeFetcher{},
		func(_ context.Context, _ string, _ []byte) error {
			t.Fatal("onMessage should not be called")
			return nil
		},
		nil,
		"pk-1",
		10,
	)
	if err != nil {
		t.Fatalf("expected nil error for empty relays, got: %v", err)
	}
}

func TestFetchAuthorMetadata_QueriesRelaysForKind0(t *testing.T) {
	t.Parallel()
	var capturedKinds []int
	var capturedAuthors [][]string
	fetcher := fakeFetcher{
		fetchPageFn: func(_ context.Context, _ string, req backfill.PageRequest) (backfill.PageResult, error) {
			capturedKinds = append(capturedKinds, req.Kinds...)
			capturedAuthors = append(capturedAuthors, req.Authors)
			return backfill.PageResult{
				Events: [][]byte{[]byte(`{"id":"e1","kind":0}`)},
			}, nil
		},
	}

	messageCount := 0
	err := fetchAuthorMetadata(
		context.Background(),
		fetcher,
		func(_ context.Context, _ string, _ []byte) error {
			messageCount++
			return nil
		},
		[]string{"wss://relay.example.com"},
		"pk-1",
		10,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(capturedKinds) != 1 || capturedKinds[0] != 0 {
		t.Fatalf("expected kinds [0], got %v", capturedKinds)
	}
	if len(capturedAuthors) != 1 || len(capturedAuthors[0]) != 1 || capturedAuthors[0][0] != "pk-1" {
		t.Fatalf("expected authors [[pk-1]], got %v", capturedAuthors)
	}
	if messageCount != 1 {
		t.Fatalf("expected 1 message processed, got %d", messageCount)
	}
}

func TestFetchAuthorMetadata_StopsAfterFirstRelayWithResults(t *testing.T) {
	t.Parallel()
	relaysCalled := 0
	fetcher := fakeFetcher{
		fetchPageFn: func(_ context.Context, _ string, _ backfill.PageRequest) (backfill.PageResult, error) {
			relaysCalled++
			return backfill.PageResult{
				Events: [][]byte{[]byte(`{"id":"e1"}`)},
			}, nil
		},
	}

	err := fetchAuthorMetadata(
		context.Background(),
		fetcher,
		func(_ context.Context, _ string, _ []byte) error { return nil },
		[]string{"wss://relay1.example.com", "wss://relay2.example.com"},
		"pk-1",
		10,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if relaysCalled != 1 {
		t.Fatalf("expected only 1 relay to be called (early exit on results), got %d", relaysCalled)
	}
}

type fakeFetcher struct {
	fetchPageFn func(ctx context.Context, relayURL string, req backfill.PageRequest) (backfill.PageResult, error)
}

func (f fakeFetcher) FetchPage(ctx context.Context, relayURL string, req backfill.PageRequest) (backfill.PageResult, error) {
	if f.fetchPageFn != nil {
		return f.fetchPageFn(ctx, relayURL, req)
	}
	return backfill.PageResult{}, nil
}
