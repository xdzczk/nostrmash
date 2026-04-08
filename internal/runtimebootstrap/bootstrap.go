package runtimebootstrap

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

// Logger is the minimal logger contract shared across binary entrypoints.
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

func ResolveAppVersion() string {
	appVersion := strings.TrimSpace(os.Getenv("APP_VERSION"))
	if appVersion == "" {
		return "dev"
	}
	return appVersion
}

func ResolveBuildVersion(appVersion, buildVersion string) string {
	if v := strings.TrimSpace(buildVersion); v != "" {
		return v
	}
	return strings.TrimSpace(appVersion)
}

func InitTracing(
	ctx context.Context,
	log Logger,
	serviceName string,
	binaryRole string,
	version string,
	environment string,
) error {
	if err := traceutil.Init(ctx, serviceName, binaryRole, version, environment); err != nil {
		return err
	}
	return nil
}

func ShutdownTracing(log Logger) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := traceutil.Shutdown(shutdownCtx); err != nil {
		log.Error("tracing_shutdown", "error", err)
	}
}

func RegisterBuildAndDeployment(
	log Logger,
	binaryRole string,
	serviceName string,
	environment string,
	version string,
	buildCommit string,
	buildTime string,
) {
	commit := strings.TrimSpace(buildCommit)
	builtAt := strings.TrimSpace(buildTime)
	metrics.RegisterBuildInfo(binaryRole, version, commit, builtAt)
	metrics.RegisterDeploymentInfo(binaryRole, serviceName, environment)
	log.Info(
		"build_info",
		"binary_role", binaryRole,
		"version", version,
		"commit", commit,
		"build_time", builtAt,
		"environment", environment,
	)
}

func StartMetricsEndpoint(ctx context.Context, log Logger, addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Info("metrics_listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics_server", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}

func StartDebugEndpoint(ctx context.Context, log Logger, addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	registerPprofHandlers(mux)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Info("debug_listening", "addr", addr, "surface", "pprof", "auth", "none_bind_private_addr")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("debug_server", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}

func registerPprofHandlers(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.Handle("GET /debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("GET /debug/pprof/block", pprof.Handler("block"))
	mux.Handle("GET /debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("GET /debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("GET /debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("GET /debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}
