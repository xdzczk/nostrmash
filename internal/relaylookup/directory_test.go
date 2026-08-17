package relaylookup

import (
	"reflect"
	"testing"
)

func TestIsDirectoryRelay(t *testing.T) {
	if !IsDirectoryRelay("wss://purplepag.es") {
		t.Fatal("expected purplepag.es to be a directory relay")
	}
	if !IsDirectoryRelay("wss://PURPLEPAG.ES/") {
		t.Fatal("expected host match to be case-insensitive")
	}
	if IsDirectoryRelay("wss://nos.lol") {
		t.Fatal("did not expect nos.lol to be a directory relay")
	}
}

func TestWithoutDirectoryRelays(t *testing.T) {
	got := WithoutDirectoryRelays([]string{
		"wss://relay.primal.net",
		"wss://purplepag.es",
		"wss://nos.lol",
	})
	want := []string{"wss://relay.primal.net", "wss://nos.lol"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestMergeEventFallbackRelays_PrefersRankedThenStatic(t *testing.T) {
	got := MergeEventFallbackRelays(
		[]string{"wss://nostr.mom", "wss://purplepag.es"},
		[]string{"wss://relay.primal.net", "wss://nos.lol", "wss://purplepag.es"},
		3,
	)
	want := []string{"wss://nostr.mom", "wss://relay.primal.net", "wss://nos.lol"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestMergeEventFallbackRelays_StaticOnlyWhenRankedEmpty(t *testing.T) {
	got := MergeEventFallbackRelays(nil, []string{"wss://purplepag.es", "wss://nos.lol"}, 2)
	want := []string{"wss://nos.lol"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}
