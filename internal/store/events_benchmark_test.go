package store

import "testing"

func BenchmarkExpandEventTags(b *testing.B) {
	tags := make([][]string, 0, 48)
	for i := 0; i < 16; i++ {
		tags = append(tags, []string{"e", "evt_ref_" + string(rune('a'+(i%26))), "wss://relay.example", "reply"})
		tags = append(tags, []string{"p", "pubkey_ref_" + string(rune('a'+(i%26))), "wss://relay.example", "mention"})
		tags = append(tags, []string{"t", "topic_" + string(rune('a'+(i%26)))})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := ExpandEventTags("evt_hot_path", tags)
		if len(rows) == 0 {
			b.Fatal("expected expanded tag rows")
		}
	}
}
