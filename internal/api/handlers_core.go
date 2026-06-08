package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store/failure"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
	"github.com/xdzczk/nostrmash/internal/transport/httpx"
)

// Health is a liveness probe: process is up; no dependency checks.
func Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready returns 200 when Postgres accepts a ping, else 503.
func Ready(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if pool == nil {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "dependency_unavailable", "database is not configured")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "dependency_unavailable", "database is not reachable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

type EventReader = any

type Handlers struct {
	service             query.Service
	pool                *pgxpool.Pool
	maxBatchSize        int
	discoveryCache      *discoveryResponseCache
	cacheConfig         discoveryCacheConfig
	cacheLookupObserver cacheLookupObserver
	hydration           config.HydrationConfig
	hydrationLimiter    *minuteRateLimiter
}

var apiErrLog = logging.New("api")

type HandlersOptions struct {
	MaxBatchSize        int
	Pool                *pgxpool.Pool
	QueryOptions        query.ServiceOptions
	DiscoveryCache      *DiscoveryCacheOptions
	CacheLookupObserver cacheLookupObserver
	Hydration           config.HydrationConfig
}

func NewHandlers(reader EventReader, maxBatchSize int) (Handlers, error) {
	return NewHandlersWithOptions(reader, HandlersOptions{MaxBatchSize: maxBatchSize})
}

func NewHandlersWithOptions(reader EventReader, options HandlersOptions) (Handlers, error) {
	maxBatchSize := options.MaxBatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 200
	}
	cacheConfig := defaultDiscoveryCacheConfig()
	if options.DiscoveryCache != nil {
		cacheConfig = cacheConfig.withOverrides(*options.DiscoveryCache)
	}
	service, err := query.NewServiceWithOptions(reader, options.QueryOptions)
	if err != nil {
		return Handlers{}, err
	}
	var discoveryCache *discoveryResponseCache
	if cacheConfig.Enabled {
		discoveryCache = newDiscoveryResponseCache(cacheConfig.MaxEntries)
	}
	lookupObserver := options.CacheLookupObserver
	if lookupObserver == nil {
		lookupObserver = func(family, endpoint string, hit bool) {
			metrics.ObservePublicResponseCacheLookup(family, endpoint, hit)
		}
	}
	var hydrationLimiter *minuteRateLimiter
	if options.Hydration.PublicEnabled && options.Hydration.RateLimitPerMinute > 0 {
		hydrationLimiter = newMinuteRateLimiter(options.Hydration.RateLimitPerMinute)
	}
	return Handlers{
		service:             service,
		pool:                options.Pool,
		maxBatchSize:        maxBatchSize,
		discoveryCache:      discoveryCache,
		cacheConfig:         cacheConfig,
		cacheLookupObserver: lookupObserver,
		hydration:           options.Hydration,
		hydrationLimiter:    hydrationLimiter,
	}, nil
}

type apiErrorEnvelope struct {
	Error apiErrorBody `json:"error"`
}

type apiErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func writeInternalError(ctx context.Context, w http.ResponseWriter, cause error) {
	logging.WithRequestID(ctx, apiErrLog).Error("unhandled_handler_error", "error", cause)
	writeError(ctx, w, http.StatusInternalServerError, "internal_error", "internal server error")
}

func writeError(ctx context.Context, w http.ResponseWriter, status int, code, message string) {
	class := failure.ClassifyHTTP(status, code)
	logging.WithRequestID(ctx, apiErrLog).Info("api_error_response",
		"failure_class", class.Class,
		"failure_reason", class.Reason,
		"status", status,
		"code", code,
		"request_id", logging.RequestIDFromContext(ctx),
		"trace_id", traceutil.TraceID(ctx),
	)
	writeJSON(w, status, apiErrorEnvelope{
		Error: apiErrorBody{
			Code:      code,
			Message:   message,
			RequestID: logging.RequestIDFromContext(ctx),
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parseBoundedPositiveInt(r *http.Request, key string, defaultValue int, maxValue int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, errors.New(key + " must be a positive integer")
	}
	if parsed > maxValue {
		return 0, errors.New(key + " exceeds maximum allowed value")
	}
	return parsed, nil
}

func parseBoundedNonNegativeInt(r *http.Request, key string, defaultValue int, maxValue int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 0 {
		return 0, errors.New(key + " must be a non-negative integer")
	}
	if parsed > maxValue {
		return 0, errors.New(key + " exceeds maximum allowed value")
	}
	return parsed, nil
}

func encodeEventCursor(cursor *query.EventCursor) (string, error) {
	if cursor == nil {
		return "", nil
	}
	return httpx.EncodeEventCursorPayload(httpx.EventCursorPayload{
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	})
}

func decodeEventCursor(value string) (*query.EventCursor, error) {
	payload, err := httpx.DecodeEventCursorPayload(value)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return &query.EventCursor{
		CreatedAt: payload.CreatedAt,
		ID:        payload.ID,
	}, nil
}

func (h Handlers) addDiscoveryTrustMetadata(payload map[string]any) {
	meta := h.service.DiscoveryTrustMetadata()
	payload["trust_mode"] = meta.TrustMode
	payload["trust_applied"] = meta.TrustApplied
	payload["result_scope"] = meta.ResultScope
}

func (h Handlers) addSearchTrustMetadata(payload map[string]any) {
	meta := h.service.SearchTrustMetadata()
	payload["trust_mode"] = meta.TrustMode
	payload["trust_applied"] = meta.TrustApplied
	payload["result_scope"] = meta.ResultScope
}

func addOpenTrustMetadata(payload map[string]any) {
	payload["trust_mode"] = "open"
	payload["trust_applied"] = false
	payload["result_scope"] = "open"
}
