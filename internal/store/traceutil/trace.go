package traceutil

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var (
	initMu      sync.Mutex
	tracer                                  = otel.Tracer("nostrmash")
	shutdownFn  func(context.Context) error = func(context.Context) error { return nil }
	initialized bool
)

// Attr is a light wrapper to keep callsites concise.
type Attr struct {
	Key   string
	Value string
}

type Span struct {
	span trace.Span
}

func KV(key, value string) Attr {
	return Attr{Key: strings.TrimSpace(key), Value: strings.TrimSpace(value)}
}

// Init configures OpenTelemetry trace provider and propagators once.
// Exporting is opt-in via OTLP env (OTEL_TRACES_EXPORTER/OTEL_EXPORTER_OTLP_ENDPOINT).
func Init(ctx context.Context, serviceName, binaryRole, version, environment string) error {
	initMu.Lock()
	defer initMu.Unlock()
	if initialized {
		return nil
	}
	initialized = true

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	if !tracesExportEnabled() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return nil
	}

	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName(strings.TrimSpace(serviceName)),
			attribute.String("nostrmash.binary_role", strings.TrimSpace(binaryRole)),
			attribute.String("deployment.environment", strings.TrimSpace(environment)),
			attribute.String("service.version", strings.TrimSpace(version)),
		),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithOS(),
		resource.WithProcess(),
	)
	if err != nil {
		return err
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	shutdownFn = provider.Shutdown
	otel.SetTracerProvider(provider)
	return nil
}

func Shutdown(ctx context.Context) error {
	return shutdownFn(ctx)
}

func StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, *Span) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "span"
	}
	spanAttrs := toOTelAttrs(attrs)
	ctx, span := tracer.Start(ctx, name, trace.WithAttributes(spanAttrs...))
	return ctx, &Span{span: span}
}

func ExtractHTTPContext(ctx context.Context, header http.Header) context.Context {
	if header == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(header))
}

func TraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	if !spanCtx.IsValid() {
		return ""
	}
	return spanCtx.TraceID().String()
}

func (s *Span) End(err error) {
	if s == nil || s.span == nil {
		return
	}
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, strings.TrimSpace(err.Error()))
	}
	s.span.End()
}

func toOTelAttrs(attrs []Attr) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		key := strings.TrimSpace(attr.Key)
		if key == "" {
			continue
		}
		out = append(out, attribute.String(key, strings.TrimSpace(attr.Value)))
	}
	return out
}

func tracesExportEnabled() bool {
	exporter := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER")))
	switch exporter {
	case "none":
		return false
	case "otlp":
		return true
	}
	return strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != ""
}
