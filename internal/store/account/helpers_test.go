package account

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestAccountConverters(t *testing.T) {
	if got := nullableTime(time.Time{}); got != nil {
		t.Fatalf("zero time should be nil, got %#v", got)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	if got := nullableTime(now); got != now {
		t.Fatalf("nullableTime = %#v", got)
	}

	if got := tsPtr(pgtype.Timestamptz{}); got != nil {
		t.Fatalf("invalid timestamptz should be nil")
	}
	if got := tsPtr(pgtype.Timestamptz{Time: now, Valid: true}); got == nil || !got.Equal(now) {
		t.Fatalf("tsPtr = %#v", got)
	}

	if got := timePtrToTS(nil); got.Valid {
		t.Fatalf("nil timePtrToTS should be invalid")
	}
	if got := timePtrToTS(&now); !got.Valid || !got.Time.Equal(now) {
		t.Fatalf("timePtrToTS = %#v", got)
	}

	if got := intPtrToInt32Ptr(nil); got != nil {
		t.Fatalf("nil intPtrToInt32Ptr should be nil")
	}
	v := 7
	if got := intPtrToInt32Ptr(&v); got == nil || *got != 7 {
		t.Fatalf("intPtrToInt32Ptr = %#v", got)
	}

	if got := int32PtrToIntPtr(nil); got != nil {
		t.Fatalf("nil int32PtrToIntPtr should be nil")
	}
	v32 := int32(9)
	if got := int32PtrToIntPtr(&v32); got == nil || *got != 9 {
		t.Fatalf("int32PtrToIntPtr = %#v", got)
	}
}

func TestAccountsNilGuards(t *testing.T) {
	var s *Accounts
	if err := s.BatchIncrementAccountObservations(t.Context(), map[string]int64{"a": 1}); err == nil {
		t.Fatal("expected nil receiver error")
	}
	s = &Accounts{}
	if err := s.BatchIncrementAccountObservations(t.Context(), map[string]int64{"a": 1}); err == nil {
		t.Fatal("expected nil pool error")
	}
	if err := s.BatchIncrementAccountObservations(t.Context(), nil); err == nil {
		t.Fatal("expected nil pool error even for empty deltas")
	}
	if _, err := s.GetAccountState(t.Context(), "pk"); err == nil {
		t.Fatal("expected nil pool error for GetAccountState")
	}
}
