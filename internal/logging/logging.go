package logging

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey string

const requestIDKey ctxKey = "request_id"

// New returns a JSON logger to stdout with optional level from LOG_LEVEL (debug, info, warn, error).
func New(service string) *slog.Logger {
	level := slog.LevelInfo
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}

// WithRequestID returns a child logger with request_id if present in context.
func WithRequestID(ctx context.Context, log *slog.Logger) *slog.Logger {
	if id, ok := ctx.Value(requestIDKey).(string); ok && id != "" {
		return log.With("request_id", id)
	}
	return log
}

// ContextWithRequestID stores request id in context.
func ContextWithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFromContext returns the request id if set.
func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}
