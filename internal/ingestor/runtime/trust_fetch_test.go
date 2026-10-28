package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
)

type recordingPageFetcher struct {
	kinds [][]int
	err   error
}

func (f *recordingPageFetcher) FetchPage(_ context.Context, _ string, request backfill.PageRequest) (backfill.PageResult, error) {
	if f.err != nil {
		return backfill.PageResult{}, f.err
	}
	f.kinds = append(f.kinds, append([]int(nil), request.Kinds...))
	return backfill.PageResult{EOSESeen: true}, nil
}

func TestFetchTrustedPubkeySliceRequestsNotesThenReplaceables(t *testing.T) {
	t.Parallel()
	fetcher := &recordingPageFetcher{}
	err := fetchTrustedPubkeySlice(
		context.Background(),
		fetcher,
		func(context.Context, string, []byte) error { return nil },
		[]string{"wss://relay.example"},
		"pk",
		172800,
		50,
	)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !reflect.DeepEqual(fetcher.kinds, trustFetchKindGroups) {
		t.Fatalf("kinds = %v, want %v", fetcher.kinds, trustFetchKindGroups)
	}
}

func TestFetchTrustedPubkeySliceErrorsWhenEveryRelayFails(t *testing.T) {
	t.Parallel()
	fetcher := &recordingPageFetcher{err: errors.New("dial failed")}
	err := fetchTrustedPubkeySlice(
		context.Background(),
		fetcher,
		func(context.Context, string, []byte) error { return nil },
		[]string{"wss://relay.example"},
		"pk",
		60,
		10,
	)
	if err == nil {
		t.Fatal("expected relay failure")
	}
}

func TestFetchTrustedPubkeySliceSucceedsWhenOneKindGroupWorks(t *testing.T) {
	t.Parallel()
	calls := 0
	fetcher := pageFetcherFunc(func(_ context.Context, _ string, request backfill.PageRequest) (backfill.PageResult, error) {
		calls++
		if request.Kinds[0] == 1 {
			return backfill.PageResult{}, errors.New("notes relay down")
		}
		return backfill.PageResult{EOSESeen: true}, nil
	})
	started := time.Now()
	err := fetchTrustedPubkeySlice(
		context.Background(),
		fetcher,
		func(context.Context, string, []byte) error { return nil },
		[]string{"wss://relay.example"},
		"pk",
		60,
		10,
	)
	if err != nil {
		t.Fatalf("partial success should not fail the pubkey: %v", err)
	}
	if calls != len(trustFetchKindGroups) {
		t.Fatalf("calls = %d", calls)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("fetch retry loop stalled")
	}
}

type pageFetcherFunc func(context.Context, string, backfill.PageRequest) (backfill.PageResult, error)

func (f pageFetcherFunc) FetchPage(ctx context.Context, relayURL string, request backfill.PageRequest) (backfill.PageResult, error) {
	return f(ctx, relayURL, request)
}
