package query

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

const (
	fallbackEntityEvent   = "event"
	fallbackEntityProfile = "profile"

	fallbackResultHit   = "hit"
	fallbackResultMiss  = "miss"
	fallbackResultError = "error"
)

func observeFallbackAttemptByEntity(entityType string) {
	metrics.IncLookupFallbackAttempt(entityType)
}

func observeFallbackResultByEntity(entityType, resultClass string, d time.Duration) {
	switch resultClass {
	case fallbackResultHit:
		metrics.IncLookupFallbackSuccess(entityType)
	case fallbackResultMiss:
		metrics.IncLookupFallbackMiss(entityType)
	case fallbackResultError:
		metrics.IncLookupFallbackFailure(entityType)
	default:
		metrics.IncLookupFallbackPartialSuccess(entityType)
	}
	metrics.ObserveLookupFallbackLatency(entityType, d)
}

func logFallbackInfraFailure(
	ctx context.Context,
	surface string,
	entityType string,
	entityKey string,
	err error,
	degradedToNotFound bool,
) {
	if err == nil {
		return
	}
	log := logging.WithRequestID(ctx, slog.Default())
	log.Warn(
		"query_fallback_lookup_failed",
		"surface", surface,
		"entity_type", entityType,
		"entity_key", strings.TrimSpace(entityKey),
		"error_class", classifyFallbackError(err),
		"error_message", err.Error(),
		"degraded_to_not_found", degradedToNotFound,
	)
}

func logFallbackBatchInfraFailure(
	ctx context.Context,
	surface string,
	entityType string,
	entityKeys []string,
	err error,
	degradedToNotFound bool,
) {
	if err == nil {
		return
	}
	keys := make([]string, 0, len(entityKeys))
	for _, key := range entityKeys {
		normalized := strings.TrimSpace(key)
		if normalized == "" {
			continue
		}
		keys = append(keys, normalized)
	}
	log := logging.WithRequestID(ctx, slog.Default())
	log.Warn(
		"query_fallback_lookup_failed",
		"surface", surface,
		"entity_type", entityType,
		"entity_keys", keys,
		"entity_count", len(keys),
		"error_class", classifyFallbackError(err),
		"error_message", err.Error(),
		"degraded_to_not_found", degradedToNotFound,
	)
}

func classifyFallbackError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "all fallback relay queries failed"):
		return "relay_exhausted"
	case strings.Contains(message, "dial relay"):
		return "transport"
	case strings.Contains(message, "write fallback req"):
		return "protocol_write"
	case strings.Contains(message, "read"):
		return "protocol_read"
	default:
		return "unknown"
	}
}
