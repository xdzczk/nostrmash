package nostr

import "testing"

func TestAuthorWriteRelaysPrefersKind10002OverKind3(t *testing.T) {
	t.Parallel()
	kind10002 := []byte(`{"kind":10002,"tags":[["r","wss://write.example","write"],["r","wss://read.example","read"]]}`)
	kind3 := []byte(`{"kind":3,"content":"{\"wss://legacy.example\":{\"read\":true,\"write\":true}}"}`)
	got := AuthorWriteRelays(kind10002, kind3)
	if len(got) != 1 || got[0] != "wss://write.example" {
		t.Fatalf("relays = %#v", got)
	}
}

func TestAuthorWriteRelaysReadOnlyListDoesNotFallThrough(t *testing.T) {
	t.Parallel()
	kind10002 := []byte(`{"kind":10002,"tags":[["r","wss://read.example","read"]]}`)
	kind3 := []byte(`{"kind":3,"content":"{\"wss://legacy.example\":{\"write\":true}}"}`)
	if got := AuthorWriteRelays(kind10002, kind3); len(got) != 0 {
		t.Fatalf("relays = %#v", got)
	}
}

func TestAuthorWriteRelaysUsesKind3ContentOrder(t *testing.T) {
	t.Parallel()
	kind3 := []byte(`{"kind":3,"content":"{\"wss://relay.damus.io\":{\"read\":true,\"write\":true},\"wss://nostr.wine\":{\"read\":true,\"write\":false},\"wss://no.str.cr\":{\"read\":true,\"write\":true}}"}`)
	got := AuthorWriteRelays(nil, kind3)
	want := []string{"wss://relay.damus.io", "wss://no.str.cr"}
	if len(got) != len(want) {
		t.Fatalf("relays = %#v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("relays = %#v, want %#v", got, want)
		}
	}
}

func TestWriteRelaysFromKind10002SkipsOtherKinds(t *testing.T) {
	t.Parallel()
	if WriteRelaysFromKind10002([]byte(`{"kind":3,"tags":[["r","wss://nope.example"]]}`)) != nil {
		t.Fatal("kind 3 was treated as a relay list")
	}
	if WriteRelaysFromKind10002([]byte(`{`)) != nil {
		t.Fatal("invalid json returned relays")
	}
}

func TestPickExtraWriteRelaysSkipsBaseDisabledAndUnprobed(t *testing.T) {
	t.Parallel()
	candidates := []string{
		"wss://relay.damus.io",
		"wss://dead.example",
		"wss://no.str.cr",
		"wss://disabled.example",
		"wss://nostr.bitcoiner.social",
		"wss://nostr.oxtr.dev",
	}
	healthy := map[string]struct{}{
		"wss://relay.damus.io":         {},
		"wss://no.str.cr":              {},
		"wss://disabled.example":       {},
		"wss://nostr.bitcoiner.social": {},
		"wss://nostr.oxtr.dev":         {},
	}
	got := PickExtraWriteRelays(candidates, []string{"wss://relay.damus.io/"}, []string{"wss://disabled.example"}, healthy, true, MaxExtraAuthorWriteRelays)
	want := []string{"wss://no.str.cr", "wss://nostr.bitcoiner.social", "wss://nostr.oxtr.dev"}
	if len(got) != len(want) {
		t.Fatalf("relays = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("relays = %#v, want %#v", got, want)
		}
	}
}

func TestNormalizedAuthorWriteRelaysDropsPrivateAndPlaintext(t *testing.T) {
	t.Parallel()
	kind3 := []byte(`{"kind":3,"content":"{\"ws://plain.example\":{\"write\":true},\"wss://127.0.0.1\":{\"write\":true},\"wss://ok.example/path\":{\"write\":true}}"}`)
	got := NormalizedAuthorWriteRelays(nil, kind3, true)
	if len(got) != 1 || got[0] != "wss://ok.example/path" {
		t.Fatalf("relays = %#v", got)
	}
}
