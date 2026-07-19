package derivation

import (
	"strings"
	"testing"
)

func i64(v int64) *int64 { return &v }

func TestNormalizeRebuildScope(t *testing.T) {
	t.Run("full scope", func(t *testing.T) {
		got, err := normalizeRebuildScope(ProjectionRebuildScope{Type: "  FULL "})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Type != RebuildScopeFull {
			t.Fatalf("type = %q want %q", got.Type, RebuildScopeFull)
		}
	})

	t.Run("event scope normalizes alias and trims id", func(t *testing.T) {
		got, err := normalizeRebuildScope(ProjectionRebuildScope{Type: "event-scoped", EventID: "  evt1 "})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Type != RebuildScopeEvent || got.EventID != "evt1" {
			t.Fatalf("unexpected scope: %+v", got)
		}
	})

	t.Run("event scope requires id", func(t *testing.T) {
		_, err := normalizeRebuildScope(ProjectionRebuildScope{Type: "event", EventID: "  "})
		if err == nil || !strings.Contains(err.Error(), "event_id is required") {
			t.Fatalf("expected event_id required error, got %v", err)
		}
	})

	t.Run("pubkey scope requires pubkey", func(t *testing.T) {
		_, err := normalizeRebuildScope(ProjectionRebuildScope{Type: "pubkey-scoped"})
		if err == nil || !strings.Contains(err.Error(), "pubkey is required") {
			t.Fatalf("expected pubkey required error, got %v", err)
		}
		got, err := normalizeRebuildScope(ProjectionRebuildScope{Type: "pubkey", Pubkey: " pk "})
		if err != nil || got.Pubkey != "pk" || got.Type != RebuildScopePubkey {
			t.Fatalf("unexpected pubkey scope: %+v err %v", got, err)
		}
	})

	t.Run("time range requires both bounds", func(t *testing.T) {
		_, err := normalizeRebuildScope(ProjectionRebuildScope{Type: "time_range", StartCreatedAt: i64(5)})
		if err == nil || !strings.Contains(err.Error(), "are required") {
			t.Fatalf("expected bounds required error, got %v", err)
		}
	})

	t.Run("time range rejects inverted bounds", func(t *testing.T) {
		_, err := normalizeRebuildScope(ProjectionRebuildScope{Type: "time-range", StartCreatedAt: i64(10), EndCreatedAt: i64(5)})
		if err == nil || !strings.Contains(err.Error(), "must be <=") {
			t.Fatalf("expected ordering error, got %v", err)
		}
	})

	t.Run("time range accepts valid bounds", func(t *testing.T) {
		got, err := normalizeRebuildScope(ProjectionRebuildScope{Type: "time_range", StartCreatedAt: i64(5), EndCreatedAt: i64(10)})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Type != RebuildScopeTimeRange || *got.StartCreatedAt != 5 || *got.EndCreatedAt != 10 {
			t.Fatalf("unexpected time range scope: %+v", got)
		}
	})

	t.Run("unknown type errors", func(t *testing.T) {
		_, err := normalizeRebuildScope(ProjectionRebuildScope{Type: "bogus"})
		if err == nil || !strings.Contains(err.Error(), "unsupported rebuild scope type") {
			t.Fatalf("expected unsupported type error, got %v", err)
		}
	})
}
