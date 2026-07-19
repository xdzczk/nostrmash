package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

// BenchmarkInsertCanonicalEventTagHeavy measures the canonical write path for a
// tag-heavy event (e.g. a large kind-3 contact list). It exercises the batched
// unnest tag insert and is DB-backed, so it auto-skips without TEST_DATABASE_URL.
func BenchmarkInsertCanonicalEventTagHeavy(b *testing.B) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(b, "bench")
	pool := dbtest.SetupSchemaPool(b, ctx, dbURL, "bench")
	if err := Migrate(ctx, pool, "bench-v1"); err != nil {
		b.Fatalf("migrate: %v", err)
	}

	st := NewPostgresStore(pool)

	// 1,000 contact ("p") tags — representative of a heavy kind-3 follow list.
	const tagCount = 1000
	tags := make([][]string, 0, tagCount)
	for i := 0; i < tagCount; i++ {
		tags = append(tags, []string{"p", fmt.Sprintf("pubkey_ref_%04d", i), "wss://relay.example"})
	}
	baseTime := time.Date(2026, 4, 4, 10, 0, 0, 0, time.UTC)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("evt_bench_%d", i)
		event := model.Event{
			ID:          id,
			Pubkey:      "bench_author",
			CreatedAt:   int64(1_700_000_000 + i),
			Kind:        3,
			Sig:         "sig_bench",
			Content:     "",
			RawJSON:     json.RawMessage(`{"id":"` + id + `","kind":3}`),
			FirstSeenAt: baseTime,
			InsertedAt:  baseTime,
		}
		if _, err := st.InsertCanonicalEventWithResult(ctx, event, tags, "wss://relay.one", baseTime); err != nil {
			b.Fatalf("insert canonical event: %v", err)
		}
	}
}

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
