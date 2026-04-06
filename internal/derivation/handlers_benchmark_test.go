package derivation

import "testing"

func BenchmarkDeriveEventReferences(b *testing.B) {
	tags := [][]string{
		{"e", "root_evt", "wss://relay.one", "root"},
		{"e", "reply_evt", "wss://relay.one", "reply"},
		{"e", "mention_evt_1"},
		{"e", "mention_evt_2", "wss://relay.two"},
		{"p", "pubkey_1"},
		{"p", "pubkey_2", "wss://relay.two", "mention"},
		{"t", "nostr"},
		{"e", "reply_evt_legacy"},
		{"e", "reply_evt_legacy_2"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		refs := deriveEventReferences("evt_source", tags)
		if len(refs) == 0 {
			b.Fatal("expected derived event references")
		}
	}
}

func BenchmarkNormalizeUniqueIDs(b *testing.B) {
	ids := []string{
		"  evt_1  ", "evt_2", "evt_3", "evt_1", "", "   ",
		"evt_4", "evt_5", "evt_5", "evt_6", "evt_7", "evt_7",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := normalizeUniqueIDs(ids)
		if len(out) == 0 {
			b.Fatal("expected normalized IDs")
		}
	}
}
