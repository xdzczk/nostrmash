package failure

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

type Class string

const (
	ClassClientInput         Class = "client_input"
	ClassDependencyTransient Class = "dependency_transient"
	ClassStorage             Class = "storage"
	ClassQueueJob            Class = "queue_job"
	ClassInternalBug         Class = "internal_bug"
	ClassUnknown             Class = "unknown"
)

type Detail struct {
	Class  Class
	Reason string
}

type PanicError struct {
	Value any
}

func (e PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

func FromPanic(v any) error {
	return PanicError{Value: v}
}

func ClassifyError(err error) Detail {
	if err == nil {
		return Detail{Class: ClassUnknown, Reason: "none"}
	}
	var panicErr PanicError
	if errors.As(err, &panicErr) {
		return Detail{Class: ClassInternalBug, Reason: "panic"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Detail{Class: ClassDependencyTransient, Reason: "deadline_exceeded"}
	}
	if errors.Is(err, context.Canceled) {
		return Detail{Class: ClassDependencyTransient, Reason: "context_canceled"}
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return Detail{Class: ClassStorage, Reason: "postgres_" + strings.TrimSpace(pgErr.Code)}
	}

	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "queue") || strings.Contains(msg, "job"):
		return Detail{Class: ClassQueueJob, Reason: "queue_or_job_error"}
	case strings.Contains(msg, "database") || strings.Contains(msg, "postgres") || strings.Contains(msg, "pgx") || strings.Contains(msg, "sql"):
		return Detail{Class: ClassStorage, Reason: "storage_error"}
	default:
		return Detail{Class: ClassInternalBug, Reason: "internal_error"}
	}
}

func ClassifyHTTP(status int, code string) Detail {
	code = strings.TrimSpace(strings.ToLower(code))
	switch {
	case status >= 400 && status < 500:
		return Detail{Class: ClassClientInput, Reason: code}
	case code == "dependency_unavailable" || status == 503:
		return Detail{Class: ClassDependencyTransient, Reason: code}
	case status >= 500:
		return Detail{Class: ClassInternalBug, Reason: code}
	default:
		return Detail{Class: ClassUnknown, Reason: code}
	}
}
