package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
	"github.com/xdzczk/nostrmash/internal/store"
)

const (
	crawlAuthor  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	crawlFollow1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	crawlFollow2 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func TestWriteRelaysFromKind10002(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"kind":10002,"tags":[["r","wss://write.example"],["r","wss://both.example","write"],["r","wss://read.example","read"],["r","wss://other.example","unknown"],["p","ignored"]]}`)
	got := writeRelaysFromKind10002(raw)
	want := []string{"wss://write.example", "wss://both.example"}
	if len(got) != len(want) {
		t.Fatalf("relays = %#v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("relays = %#v, want %#v", got, want)
		}
	}
	if writeRelaysFromKind10002([]byte(`{"kind":3,"tags":[["r","wss://nope.example"]]}`)) != nil {
		t.Fatal("kind 3 was treated as a relay list")
	}
	if writeRelaysFromKind10002([]byte(`{`)) != nil {
		t.Fatal("invalid json returned relays")
	}
}

func TestSelectContactCrawlRelays(t *testing.T) {
	t.Parallel()
	allow := []string{"wss://write.example", "wss://second.example", "wss://third.example", "wss://fourth.example"}
	got := selectContactCrawlRelays(
		[]string{"wss://WRITE.example/", "wss://read-not-allow.example", "wss://second.example", "wss://third.example", "wss://fourth.example"},
		[]string{"wss://fallback.example"},
		allow,
		[]string{"wss://second.example"},
		true,
		3,
	)
	want := []string{"wss://write.example", "wss://third.example", "wss://fourth.example"}
	if len(got) != len(want) {
		t.Fatalf("relays = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("relays = %#v, want %#v", got, want)
		}
	}

	fallback := selectContactCrawlRelays(
		[]string{"wss://outside.example", "ws://insecure.example"},
		[]string{"wss://fallback.example", "wss://disabled.example", "ws://skip.example"},
		[]string{"wss://fallback.example"},
		[]string{"wss://disabled.example"},
		true,
		3,
	)
	if len(fallback) != 1 || fallback[0] != "wss://fallback.example" {
		t.Fatalf("fallback = %#v", fallback)
	}
}

func TestFetchContactList_RequestsCurrentListAndStops(t *testing.T) {
	t.Parallel()
	var requests []backfill.PageRequest
	var relays []string
	fetcher := fakeFetcher{
		fetchPageFn: func(_ context.Context, relayURL string, req backfill.PageRequest) (backfill.PageResult, error) {
			relays = append(relays, relayURL)
			requests = append(requests, req)
			if relayURL == "wss://first.example" {
				return backfill.PageResult{
					Events:   [][]byte{[]byte(`{"kind":10002,"tags":[["r","wss://first.example"]]}`)},
					EOSESeen: true,
				}, nil
			}
			return backfill.PageResult{
				Events: [][]byte{
					[]byte(`{"kind":10002,"tags":[["r","wss://second.example"]]}`),
					[]byte(`{"kind":3,"created_at":10,"tags":[["p","` + crawlFollow1 + `"],["p","not-hex"],["p","` + crawlAuthor + `"],["p","` + crawlFollow1 + `"],["p","` + crawlFollow2 + `"]]}`),
				},
				EOSESeen: true,
			}, nil
		},
	}
	var persisted int
	found, contacts, err := fetchContactList(
		context.Background(),
		fetcher,
		func(context.Context, string, []byte) error {
			persisted++
			return nil
		},
		[]string{"wss://first.example", "wss://second.example", "wss://third.example"},
		crawlAuthor,
	)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !found {
		t.Fatal("expected a contact list")
	}
	if len(relays) != 2 || relays[1] != "wss://second.example" {
		t.Fatalf("relays = %#v", relays)
	}
	if len(requests) != 2 || requests[0].Since != nil || requests[0].Until != nil || requests[0].Limit != 2 {
		t.Fatalf("request = %#v", requests[0])
	}
	if len(requests[0].Kinds) != 2 || requests[0].Kinds[0] != 3 || requests[0].Kinds[1] != 10002 {
		t.Fatalf("kinds = %#v", requests[0].Kinds)
	}
	if len(requests[0].Authors) != 1 || requests[0].Authors[0] != crawlAuthor {
		t.Fatalf("authors = %#v", requests[0].Authors)
	}
	if persisted != 3 {
		t.Fatalf("persisted %d events, want 3", persisted)
	}
	if len(contacts) != 2 || contacts[0] != crawlFollow1 || contacts[1] != crawlFollow2 {
		t.Fatalf("contacts = %#v", contacts)
	}
}

func TestFetchContactList_ConfirmedMissAndRelayErrors(t *testing.T) {
	t.Parallel()
	missFetcher := fakeFetcher{
		fetchPageFn: func(context.Context, string, backfill.PageRequest) (backfill.PageResult, error) {
			return backfill.PageResult{EOSESeen: true}, nil
		},
	}
	found, contacts, err := fetchContactList(context.Background(), missFetcher, func(context.Context, string, []byte) error { return nil }, []string{"wss://relay.example"}, crawlAuthor)
	if err != nil || found || len(contacts) != 0 {
		t.Fatalf("miss found=%v contacts=%v err=%v", found, contacts, err)
	}

	errFetcher := fakeFetcher{
		fetchPageFn: func(context.Context, string, backfill.PageRequest) (backfill.PageResult, error) {
			return backfill.PageResult{}, errors.New("dial failed")
		},
	}
	_, _, err = fetchContactList(context.Background(), errFetcher, func(context.Context, string, []byte) error { return nil }, []string{"wss://relay.example"}, crawlAuthor)
	if err == nil {
		t.Fatal("expected relay errors to retry")
	}

	mixed := fakeFetcher{
		fetchPageFn: func(_ context.Context, relayURL string, _ backfill.PageRequest) (backfill.PageResult, error) {
			if relayURL == "wss://down.example" {
				return backfill.PageResult{}, errors.New("dial failed")
			}
			return backfill.PageResult{EOSESeen: true}, nil
		},
	}
	_, _, err = fetchContactList(context.Background(), mixed, func(context.Context, string, []byte) error { return nil }, []string{"wss://down.example", "wss://empty.example"}, crawlAuthor)
	if err == nil {
		t.Fatal("expected a failed relay plus an empty EOSE to retry")
	}

	idle := fakeFetcher{
		fetchPageFn: func(context.Context, string, backfill.PageRequest) (backfill.PageResult, error) {
			return backfill.PageResult{}, nil
		},
	}
	_, _, err = fetchContactList(context.Background(), idle, func(context.Context, string, []byte) error { return nil }, []string{"wss://relay.example"}, crawlAuthor)
	if err == nil {
		t.Fatal("expected a close without EOSE to retry")
	}
}

func TestRunContactListCrawlCycle_UsesWriteRelayAndEnqueuesNextHop(t *testing.T) {
	t.Parallel()
	st := &fakeContactCrawlStore{
		claimed: []store.ContactListCrawlEntry{{Pubkey: crawlAuthor, Hop: 0}},
		raw:     []byte(`{"kind":10002,"tags":[["r","wss://write.example","write"],["r","wss://read.example","read"]]}`),
	}
	var dialed []string
	fetcher := fakeFetcher{
		fetchPageFn: func(_ context.Context, relayURL string, req backfill.PageRequest) (backfill.PageResult, error) {
			dialed = append(dialed, relayURL)
			if req.Since != nil || req.Limit != contactListPageLimit {
				t.Fatalf("request = %#v", req)
			}
			return backfill.PageResult{
				Events:   [][]byte{[]byte(`{"kind":3,"tags":[["p","` + crawlFollow1 + `"]]}`)},
				EOSESeen: true,
			}, nil
		},
	}
	runContactListCrawlCycle(
		context.Background(),
		nil,
		st,
		config.IngestorContactCrawlConfig{
			Enabled:            true,
			BatchSize:          20,
			MaxHops:            2,
			MaxPending:         100,
			MaxRelaysPerPubkey: 3,
			RefetchInterval:    time.Hour,
			RetryDelay:         time.Minute,
		},
		[]string{"wss://fallback.example"},
		[]string{"wss://write.example", "wss://fallback.example"},
		nil,
		true,
		fetcher,
		func(context.Context, string, []byte) error { return nil },
	)
	if len(dialed) != 1 || dialed[0] != "wss://write.example" {
		t.Fatalf("dialed %#v", dialed)
	}
	if st.found == nil || !*st.found {
		t.Fatalf("found = %v", st.found)
	}
	if st.enqueuedHop != 1 || len(st.enqueued) != 1 || st.enqueued[0] != crawlFollow1 {
		t.Fatalf("enqueued hop=%d pubkeys=%v", st.enqueuedHop, st.enqueued)
	}
	if st.failed != nil {
		t.Fatalf("unexpected failure: %v", st.failed)
	}
}

func TestRunContactListCrawlCycle_DoesNotEnqueuePastMaxHops(t *testing.T) {
	t.Parallel()
	st := &fakeContactCrawlStore{
		claimed: []store.ContactListCrawlEntry{{Pubkey: crawlAuthor, Hop: 2}},
	}
	fetcher := fakeFetcher{
		fetchPageFn: func(context.Context, string, backfill.PageRequest) (backfill.PageResult, error) {
			return backfill.PageResult{
				Events:   [][]byte{[]byte(`{"kind":3,"tags":[["p","` + crawlFollow1 + `"]]}`)},
				EOSESeen: true,
			}, nil
		},
	}
	runContactListCrawlCycle(
		context.Background(),
		nil,
		st,
		config.IngestorContactCrawlConfig{
			Enabled:            true,
			BatchSize:          1,
			MaxHops:            2,
			MaxPending:         100,
			MaxRelaysPerPubkey: 3,
			RefetchInterval:    time.Hour,
			RetryDelay:         time.Minute,
		},
		[]string{"wss://fallback.example"},
		[]string{"wss://fallback.example"},
		nil,
		true,
		fetcher,
		func(context.Context, string, []byte) error { return nil },
	)
	if len(st.enqueued) != 0 {
		t.Fatalf("enqueued past max hops: %#v", st.enqueued)
	}
	if st.found == nil || !*st.found {
		t.Fatalf("found = %v", st.found)
	}
}

type fakeContactCrawlStore struct {
	claimed     []store.ContactListCrawlEntry
	raw         []byte
	enqueuedHop int
	enqueued    []string
	found       *bool
	failed      error
}

func (f *fakeContactCrawlStore) SeedContactListCrawl(context.Context, int, time.Duration) (int, error) {
	return 0, nil
}

func (f *fakeContactCrawlStore) ClaimContactListCrawl(context.Context, int, time.Duration) ([]store.ContactListCrawlEntry, error) {
	return f.claimed, nil
}

func (f *fakeContactCrawlStore) MarkContactListCrawlFetched(_ context.Context, _ string, foundList bool, _ time.Duration) error {
	f.found = &foundList
	return nil
}

func (f *fakeContactCrawlStore) MarkContactListCrawlFailed(_ context.Context, _ string, _ time.Duration, cause error) error {
	f.failed = cause
	return nil
}

func (f *fakeContactCrawlStore) EnqueueContactListCrawl(_ context.Context, pubkeys []string, hop int, _ int) (int, error) {
	f.enqueuedHop = hop
	f.enqueued = append([]string(nil), pubkeys...)
	return len(pubkeys), nil
}

func (f *fakeContactCrawlStore) ContactListCrawlDueByHop(context.Context) (map[int]int, error) {
	return map[int]int{0: 1}, nil
}

func (f *fakeContactCrawlStore) LatestKind10002Raw(context.Context, string) ([]byte, error) {
	return f.raw, nil
}
