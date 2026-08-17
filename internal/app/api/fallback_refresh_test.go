package appapi

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/relaylookup"
)

type stubEventFallbackRanker struct {
	urls []string
	err  error
}

func (s stubEventFallbackRanker) ListFastHealthyLookupRelays(context.Context, int) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	return append([]string(nil), s.urls...), nil
}

func TestRefreshEventFallbackRelays_PrefersRankedHealthyRelays(t *testing.T) {
	client := relaylookup.NewSplitClient(relaylookup.Config{
		EventURLs:   []string{"wss://relay.primal.net", "wss://purplepag.es"},
		ProfileURLs: []string{"wss://purplepag.es"},
		Timeout:     time.Second,
		MaxFanout:   3,
	})
	refreshEventFallbackRelays(
		context.Background(),
		slog.Default(),
		client,
		stubEventFallbackRanker{urls: []string{"wss://nostr.mom", "wss://purplepag.es", "wss://nos.lol"}},
		[]string{"wss://relay.primal.net", "wss://nos.lol"},
		3,
	)
	got := client.EventRelays()
	want := []string{"wss://nostr.mom", "wss://nos.lol", "wss://relay.primal.net"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event relays: got %#v want %#v", got, want)
	}
	if profiles := client.ProfileRelays(); !reflect.DeepEqual(profiles, []string{"wss://purplepag.es"}) {
		t.Fatalf("profile relays must stay directory-only, got %#v", profiles)
	}
}

func TestRefreshEventFallbackRelays_KeepsStaticFloorOnRegistryError(t *testing.T) {
	client := relaylookup.NewSplitClient(relaylookup.Config{
		EventURLs:   []string{"wss://nos.lol", "wss://purplepag.es"},
		ProfileURLs: []string{"wss://purplepag.es"},
		Timeout:     time.Second,
		MaxFanout:   3,
	})
	before := client.EventRelays()
	refreshEventFallbackRelays(
		context.Background(),
		slog.Default(),
		client,
		stubEventFallbackRanker{err: errors.New("registry unavailable")},
		[]string{"wss://nos.lol"},
		3,
	)
	if got := client.EventRelays(); !reflect.DeepEqual(got, before) {
		t.Fatalf("expected static floor to remain, got %#v want %#v", got, before)
	}
}
