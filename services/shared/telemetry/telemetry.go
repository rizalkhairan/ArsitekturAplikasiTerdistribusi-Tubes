// Package telemetry supplies shared M2 logging, metrics and trace propagation.
package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const CorrelationHeader = "X-Correlation-ID"

type correlationKey struct{}

// Config deliberately avoids global providers so tests and services are isolated.
// Exporter ownership transfers to Runtime, which shuts it down.
type Config struct {
	Service     string
	Instance    string
	Environment string
	Output      io.Writer
	Exporter    sdktrace.SpanExporter
	SampleRatio float64
}

type Runtime struct {
	Logger     *slog.Logger
	Registry   *prometheus.Registry
	Provider   *sdktrace.TracerProvider
	Tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	requests   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	inflight   prometheus.Gauge
}

func New(cfg Config) (*Runtime, error) {
	if cfg.Service == "" || math.IsNaN(cfg.SampleRatio) || cfg.SampleRatio < 0 || cfg.SampleRatio > 1 {
		return nil, fmt.Errorf("service required; sample ratio must be 0..1")
	}
	if cfg.Output == nil {
		cfg.Output = os.Stdout
	}
	res := resource.NewSchemaless(attribute.String("service.name", cfg.Service),
		attribute.String("service.instance.id", cfg.Instance),
		attribute.String("deployment.environment.name", cfg.Environment))
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio)))}
	if cfg.Exporter != nil {
		opts = append(opts, sdktrace.WithBatcher(cfg.Exporter))
	}
	provider := sdktrace.NewTracerProvider(opts...)
	labels := prometheus.Labels{"service": cfg.Service}
	t := &Runtime{Logger: slog.New(slog.NewJSONHandler(cfg.Output, nil)).With("service", cfg.Service,
		"instance", cfg.Instance, "environment", cfg.Environment), Registry: prometheus.NewRegistry(),
		Provider: provider, Tracer: provider.Tracer("bnpb/shared"), propagator: propagation.TraceContext{}}
	t.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bnpb_http_requests_total",
		Help: "Completed HTTP requests by service, bounded route, method and status.", ConstLabels: labels}, []string{"route", "method", "status_code"})
	t.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "bnpb_http_request_duration_seconds",
		Help: "HTTP handler duration in seconds.", ConstLabels: labels,
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}}, []string{"route", "method"})
	t.inflight = prometheus.NewGauge(prometheus.GaugeOpts{Name: "bnpb_http_requests_in_flight",
		Help: "Requests currently executing.", ConstLabels: labels})
	t.Registry.MustRegister(t.requests, t.duration, t.inflight, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return t, nil
}

// FromEnv supports none (local default), stdout, or OTLP/HTTP using standard
// OTEL_EXPORTER_OTLP_* variables. A bounded batch queue keeps exporter failures
// off the synchronous request path. Errors never include credentials.
func FromEnv(ctx context.Context) (*Runtime, error) {
	ratio := 1.0
	if value := os.Getenv("BNPB_TRACE_SAMPLE_RATIO"); value != "" {
		var err error
		ratio, err = strconv.ParseFloat(value, 64)
		if err != nil || !(ratio >= 0 && ratio <= 1) {
			return nil, fmt.Errorf("invalid BNPB_TRACE_SAMPLE_RATIO")
		}
	}
	service := os.Getenv("OTEL_SERVICE_NAME")
	if service == "" {
		return nil, fmt.Errorf("OTEL_SERVICE_NAME required")
	}
	var exporter sdktrace.SpanExporter
	var err error
	switch os.Getenv("OTEL_TRACES_EXPORTER") {
	case "", "none":
	case "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithWriter(os.Stderr))
	case "otlp":
		exporter, err = otlptracehttp.New(ctx)
	default:
		return nil, fmt.Errorf("OTEL_TRACES_EXPORTER must be none, stdout or otlp")
	}
	if err != nil {
		return nil, fmt.Errorf("initialize trace exporter: %w", err)
	}
	return New(Config{Service: service, Instance: os.Getenv("POD_NAME"),
		Environment: os.Getenv("DEPLOYMENT_ENVIRONMENT"), SampleRatio: ratio, Exporter: exporter})
}

func (t *Runtime) Shutdown(ctx context.Context) error { return t.Provider.Shutdown(ctx) }
func (t *Runtime) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(t.Registry, promhttp.HandlerOpts{})
}

func validCorrelation(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

// WithCorrelation sanitizes identifiers received from HTTP, gRPC or broker headers.
func WithCorrelation(ctx context.Context, id string) context.Context {
	if !validCorrelation(id) {
		id = hex.EncodeToString(randomBytes())
	}
	return context.WithValue(ctx, correlationKey{}, id)
}

func randomBytes() []byte {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("generate correlation ID: " + err.Error())
	}
	return b
}

func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

func (t *Runtime) Log(ctx context.Context) *slog.Logger {
	sc := trace.SpanContextFromContext(ctx)
	logger := t.Logger.With("correlation_id", CorrelationID(ctx))
	if sc.IsValid() {
		logger = logger.With("trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
	}
	return logger
}

// Inject/Extract work with HTTP HeaderCarrier, gRPC metadata adapters, and a
// propagation.MapCarrier converted to Kafka headers. Baggage is intentionally omitted.
func (t *Runtime) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	t.propagator.Inject(ctx, carrier)
	if id := CorrelationID(ctx); id != "" {
		carrier.Set("x-correlation-id", id)
	}
}

func (t *Runtime) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	ctx = t.propagator.Extract(ctx, carrier)
	return WithCorrelation(ctx, carrier.Get("x-correlation-id"))
}

// Handler takes a constant route template, never a request URL or event ID.
// Mount metrics separately, outside this wrapper, to exclude scrapes from RED metrics.
func (t *Runtime) Handler(route string, next http.Handler) http.Handler {
	access := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, status, wrote := time.Now(), 200, false
		method := r.Method
		switch method {
		case "GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS":
		default:
			method = "OTHER"
		}
		t.inflight.Inc()
		defer func() {
			failure := recover()
			if failure != nil {
				status = 500
			}
			elapsed := time.Since(start).Seconds()
			t.inflight.Dec()
			t.requests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
			t.duration.WithLabelValues(route, method).Observe(elapsed)
			if status >= 500 {
				trace.SpanFromContext(r.Context()).SetStatus(codes.Error, "HTTP server error")
			}
			t.Log(r.Context()).Info("http_request", "route", route, "method", method, "status_code", status, "duration_seconds", elapsed)
			if failure != nil {
				panic(failure)
			}
		}()
		wrapped := httpsnoop.Wrap(w, httpsnoop.Hooks{
			WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
				return func(code int) {
					if !wrote && (code >= 200 || code == 101) {
						status, wrote = code, true
					}
					next(code)
				}
			},
			Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
				return func(b []byte) (int, error) { wrote = true; return next(b) }
			},
			Flush: func(next httpsnoop.FlushFunc) httpsnoop.FlushFunc { return func() { wrote = true; next() } },
		})
		next.ServeHTTP(wrapped, r)
	})
	traced := otelhttp.NewHandler(access, route, otelhttp.WithTracerProvider(t.Provider), otelhttp.WithPropagators(t.propagator))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := WithCorrelation(r.Context(), r.Header.Get(CorrelationHeader))
		w.Header().Set(CorrelationHeader, CorrelationID(ctx))
		traced.ServeHTTP(w, r.WithContext(ctx))
	})
}

type transport struct{ base http.RoundTripper }

func (tr transport) RoundTrip(r *http.Request) (*http.Response, error) {
	clone := r.Clone(r.Context())
	if id := CorrelationID(r.Context()); id != "" {
		clone.Header.Set(CorrelationHeader, id)
	}
	return tr.base.RoundTrip(clone)
}

// Transport propagates context and creates child client spans. Requests must use
// NewRequestWithContext. Callers set timeouts and close response bodies.
func (t *Runtime) Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return transport{otelhttp.NewTransport(base, otelhttp.WithTracerProvider(t.Provider), otelhttp.WithPropagators(t.propagator))}
}
