package retention

import (
	"errors"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func TestRetentionHelpers(t *testing.T) {
	if got := dbResultFromErr(nil); got != "ok" {
		t.Fatalf("dbResultFromErr(nil) = %q", got)
	}
	if got := dbResultFromErr(readmodel.ErrNotFound); got != "not_found" {
		t.Fatalf("dbResultFromErr(not found) = %q", got)
	}
	if got := dbResultFromErr(errors.New("boom")); got != "error" {
		t.Fatalf("dbResultFromErr(other) = %q", got)
	}
	if !tsz(time.Unix(1, 0).UTC()).Valid {
		t.Fatal("tsz should mark valid")
	}
	if New(nil) == nil {
		t.Fatal("New should return store")
	}
}
