package failure

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassifyError_Panic(t *testing.T) {
	got := ClassifyError(FromPanic("boom"))
	if got.Class != ClassInternalBug || got.Reason != "panic" {
		t.Fatalf("unexpected classification: %+v", got)
	}
}

func TestClassifyError_Storage(t *testing.T) {
	got := ClassifyError(&pgconn.PgError{Code: "23505"})
	if got.Class != ClassStorage {
		t.Fatalf("unexpected class: %+v", got)
	}
}

func TestClassifyError_Transient(t *testing.T) {
	got := ClassifyError(context.DeadlineExceeded)
	if got.Class != ClassDependencyTransient {
		t.Fatalf("unexpected class: %+v", got)
	}
}

func TestClassifyHTTP_ClientInput(t *testing.T) {
	got := ClassifyHTTP(400, "invalid_request")
	if got.Class != ClassClientInput {
		t.Fatalf("unexpected class: %+v", got)
	}
}
