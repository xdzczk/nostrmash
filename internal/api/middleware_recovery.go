package api

import (
	"log/slog"
	"net/http"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/store/failure"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

// WithPanicRecovery converts unexpected panics into API-safe 500 responses.
func WithPanicRecovery(log *slog.Logger, next http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err := failure.FromPanic(recovered)
				class := failure.ClassifyError(err)
				logging.WithRequestID(r.Context(), log).Error("http_panic_recovered",
					"failure_class", class.Class,
					"failure_reason", class.Reason,
					"path", r.URL.Path,
					"method", r.Method,
					"trace_id", traceutil.TraceID(r.Context()),
					"error", err,
				)
				writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
