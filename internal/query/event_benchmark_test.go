package query

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func BenchmarkServiceGetEventBatch(b *testing.B) {
	ids := make([]string, 0, 240)
	for i := 0; i < 200; i++ {
		ids = append(ids, fmt.Sprintf("evt_%03d", i))
	}
	ids = append(ids, "evt_010", "evt_010", "evt_199")

	rows := make(map[string]json.RawMessage, 200)
	for i := 0; i < 180; i++ {
		id := fmt.Sprintf("evt_%03d", i)
		rows[id] = json.RawMessage(fmt.Sprintf(`{"id":"%s","kind":1}`, id))
	}

	svc := NewEventService(fakeEventReader{
		getEventBatchFn: func(_ context.Context, requested []string) (map[string]json.RawMessage, error) {
			out := make(map[string]json.RawMessage, len(requested))
			for _, id := range requested {
				if raw, ok := rows[id]; ok {
					out[id] = raw
				}
			}
			return out, nil
		},
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := svc.GetEvents(context.Background(), ids)
		if err != nil {
			b.Fatalf("GetEvents: %v", err)
		}
		if len(out) == 0 {
			b.Fatal("expected non-empty event batch result")
		}
	}
}
