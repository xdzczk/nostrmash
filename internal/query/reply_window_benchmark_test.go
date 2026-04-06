package query

import (
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func BenchmarkWindowDescendingReplies(b *testing.B) {
	base := makeRepliesRange(1, 500)
	extra := makeRepliesRange(501, 520)
	cur := &store.EventOrderCursor{CreatedAt: 400, ID: "reply_400"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out, next := WindowDescendingReplies(base, extra, 50, cur, 0)
		if len(out) == 0 {
			b.Fatal("expected window")
		}
		_ = next
	}
}
