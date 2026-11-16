// Package obs is the thin observability shim owned by Person B.
//
// It deliberately depends only on the Go standard library so it can be swapped for
// Person A's shared library later. The public surface is intentionally small:
//
//   - Logger: structured JSON logs with correlation ID and W3C trace IDs.
//   - Registry: Prometheus text exposition (counters, gauges, histograms).
//   - Trace helpers: W3C traceparent parse/inject, span duration logs, optional
//     OTLP/HTTP JSON export.
//   - HTTP middleware: RED metrics, in-flight gauge, correlation and trace headers.
//
// Replacement contract: keep Logger, Registry, StartSpan, TraceFromContext and
// HTTPMiddleware names/semantics, or adapt at the call sites.
package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

type Logger struct {
	Service string
	Out     io.Writer
	mu      sync.Mutex
}

func NewLogger(service string) *Logger {
	return &Logger{Service: service, Out: os.Stdout}
}

func (l *Logger) Info(ctx context.Context, msg string, kv ...any) {
	l.write(ctx, "info", msg, kv...)
}

func (l *Logger) Warn(ctx context.Context, msg string, kv ...any) {
	l.write(ctx, "warn", msg, kv...)
}

func (l *Logger) Error(ctx context.Context, msg string, kv ...any) {
	l.write(ctx, "error", msg, kv...)
}

func (l *Logger) write(ctx context.Context, level, msg string, kv ...any) {
	fields := map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"level":   level,
		"service": l.Service,
		"msg":     msg,
	}
	if id := CorrelationID(ctx); id != "" {
		fields["corr_id"] = id
	}
	if tc, ok := TraceFromContext(ctx); ok {
		fields["trace_id"] = tc.TraceID
		fields["span_id"] = tc.SpanID
	}
	for i := 0; i+1 < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			continue
		}
		fields[key] = kv[i+1]
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		raw = []byte(`{"level":"error","msg":"log marshal failed"}`)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.Out.Write(append(raw, '\n'))
}

// ---------------------------------------------------------------------------
// Metrics (Prometheus text format)
// ---------------------------------------------------------------------------

type Registry struct {
	service string
	mu      sync.RWMutex
	metrics []metric
}

type metric interface {
	name() string
	render(w io.Writer)
}

func NewRegistry(service string) *Registry {
	return &Registry{service: service}
}

type CounterVec struct {
	metricName string
	help       string
	labels     []string
	mu         sync.RWMutex
	values     map[string]*counterCell
}

type counterCell struct {
	labelValues []string
	value       atomic.Uint64
}

func (r *Registry) Counter(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{metricName: name, help: help, labels: labels, values: map[string]*counterCell{}}
	r.register(c)
	return c
}

func (c *CounterVec) WithLabelValues(values ...string) *CounterCell {
	key := strings.Join(values, "\x00")
	c.mu.RLock()
	cell, ok := c.values[key]
	c.mu.RUnlock()
	if ok {
		return &CounterCell{cell: cell}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if cell, ok = c.values[key]; ok {
		return &CounterCell{cell: cell}
	}
	cell = &counterCell{labelValues: values}
	c.values[key] = cell
	return &CounterCell{cell: cell}
}

type CounterCell struct{ cell *counterCell }

func (c *CounterCell) Inc()             { c.cell.value.Add(1) }
func (c *CounterCell) Add(delta uint64) { c.cell.value.Add(delta) }

func (c *CounterVec) name() string { return c.metricName }

func (c *CounterVec) render(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.metricName, c.help, c.metricName)
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, cell := range c.values {
		writeSample(w, c.metricName, c.labels, cell.labelValues, float64(cell.value.Load()))
	}
}

type GaugeVec struct {
	metricName string
	help       string
	labels     []string
	mu         sync.RWMutex
	values     map[string]*gaugeCell
}

type gaugeCell struct {
	labelValues []string
	value       atomic.Uint64
}

func (r *Registry) Gauge(name, help string, labels ...string) *GaugeVec {
	g := &GaugeVec{metricName: name, help: help, labels: labels, values: map[string]*gaugeCell{}}
	r.register(g)
	return g
}

func (g *GaugeVec) WithLabelValues(values ...string) *GaugeCell {
	key := strings.Join(values, "\x00")
	g.mu.RLock()
	cell, ok := g.values[key]
	g.mu.RUnlock()
	if ok {
		return &GaugeCell{cell: cell}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if cell, ok = g.values[key]; ok {
		return &GaugeCell{cell: cell}
	}
	cell = &gaugeCell{labelValues: values}
	g.values[key] = cell
	return &GaugeCell{cell: cell}
}

type GaugeCell struct{ cell *gaugeCell }

func (g *GaugeCell) Set(v float64) {
	g.cell.value.Store(mathFloat64bits(v))
}
func (g *GaugeCell) Add(v float64) {
	for {
		old := g.cell.value.Load()
		next := mathFloat64bits(mathFloat64frombits(old) + v)
		if g.cell.value.CompareAndSwap(old, next) {
			return
		}
	}
}

func (g *GaugeVec) name() string { return g.metricName }

func (g *GaugeVec) render(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", g.metricName, g.help, g.metricName)
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, cell := range g.values {
		writeSample(w, g.metricName, g.labels, cell.labelValues, mathFloat64frombits(cell.value.Load()))
	}
}

type HistogramVec struct {
	metricName string
	help       string
	labels     []string
	buckets    []float64
	mu         sync.RWMutex
	values     map[string]*histCell
}

type histCell struct {
	labelValues []string
	counts      []atomic.Uint64
	count       atomic.Uint64
	sumBits     atomic.Uint64
}

func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if len(buckets) == 0 {
		buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}
	h := &HistogramVec{metricName: name, help: help, labels: labels, buckets: buckets, values: map[string]*histCell{}}
	r.register(h)
	return h
}

func (h *HistogramVec) WithLabelValues(values ...string) *HistogramCell {
	key := strings.Join(values, "\x00")
	h.mu.RLock()
	cell, ok := h.values[key]
	h.mu.RUnlock()
	if ok {
		return &HistogramCell{h: h, cell: cell}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if cell, ok = h.values[key]; ok {
		return &HistogramCell{h: h, cell: cell}
	}
	cell = &histCell{labelValues: values, counts: make([]atomic.Uint64, len(h.buckets))}
	h.values[key] = cell
	return &HistogramCell{h: h, cell: cell}
}

type HistogramCell struct {
	h    *HistogramVec
	cell *histCell
}

func (c *HistogramCell) Observe(v float64) {
	for i, b := range c.h.buckets {
		if v <= b {
			c.cell.counts[i].Add(1)
		}
	}
	c.cell.count.Add(1)
	for {
		old := c.cell.sumBits.Load()
		next := mathFloat64bits(mathFloat64frombits(old) + v)
		if c.cell.sumBits.CompareAndSwap(old, next) {
			return
		}
	}
}

func (h *HistogramVec) name() string { return h.metricName }

func (h *HistogramVec) render(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.metricName, h.help, h.metricName)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, cell := range h.values {
		for i, b := range h.buckets {
			labels := append(append([]string{}, h.labels...), "le")
			values := append(append([]string{}, cell.labelValues...), strconv.FormatFloat(b, 'g', -1, 64))
			writeSample(w, h.metricName+"_bucket", labels, values, float64(cell.counts[i].Load()))
		}
		labels := append(append([]string{}, h.labels...), "le")
		values := append(append([]string{}, cell.labelValues...), "+Inf")
		writeSample(w, h.metricName+"_bucket", labels, values, float64(cell.count.Load()))
		writeSample(w, h.metricName+"_sum", h.labels, cell.labelValues, mathFloat64frombits(cell.sumBits.Load()))
		writeSample(w, h.metricName+"_count", h.labels, cell.labelValues, float64(cell.count.Load()))
	}
}

func (r *Registry) register(m metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, m)
}

// Handler exposes the registry in Prometheus text format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.mu.RLock()
		metrics := append([]metric{}, r.metrics...)
		r.mu.RUnlock()
		for _, m := range metrics {
			m.render(w)
		}
	})
}

func writeSample(w io.Writer, name string, labelNames, labelValues []string, value float64) {
	if len(labelNames) == 0 {
		fmt.Fprintf(w, "%s %s\n", name, strconv.FormatFloat(value, 'g', -1, 64))
		return
	}
	parts := make([]string, 0, len(labelNames))
	for i := range labelNames {
		v := ""
		if i < len(labelValues) {
			v = labelValues[i]
		}
		parts = append(parts, fmt.Sprintf(`%s="%s"`, labelNames[i], escapeLabel(v)))
	}
	fmt.Fprintf(w, "%s{%s} %s\n", name, strings.Join(parts, ","), strconv.FormatFloat(value, 'g', -1, 64))
}

func escapeLabel(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

// ---------------------------------------------------------------------------
// W3C traceparent + spans
// ---------------------------------------------------------------------------

type TraceContext struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Sampled      bool
}

type traceKey struct{}

type correlationKey struct{}

// WithCorrelationID stores a correlation ID in the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

func CorrelationID(ctx context.Context) string {
	if v, ok := ctx.Value(correlationKey{}).(string); ok {
		return v
	}
	return ""
}

func TraceFromContext(ctx context.Context) (TraceContext, bool) {
	tc, ok := ctx.Value(traceKey{}).(TraceContext)
	return tc, ok
}

func WithTraceContext(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, traceKey{}, tc)
}

// ParseTraceparent parses version 00 W3C traceparent headers.
func ParseTraceparent(header string) (TraceContext, bool) {
	parts := strings.Split(strings.TrimSpace(header), "-")
	if len(parts) != 4 || parts[0] != "00" {
		return TraceContext{}, false
	}
	if len(parts[1]) != 32 || len(parts[2]) != 16 {
		return TraceContext{}, false
	}
	if !isHex(parts[1]) || !isHex(parts[2]) {
		return TraceContext{}, false
	}
	sampled := parts[3] == "01"
	return TraceContext{TraceID: parts[1], SpanID: parts[2], Sampled: sampled}, true
}

func NewTraceparent(tc TraceContext) string {
	flags := "00"
	if tc.Sampled {
		flags = "01"
	}
	return "00-" + tc.TraceID + "-" + tc.SpanID + "-" + flags
}

func NewTraceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func NewSpanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type Span struct {
	Name     string
	Trace    TraceContext
	Start    time.Time
	logger   *Logger
	export   *spanExporter
	attrs    map[string]any
	finished atomic.Bool
}

// StartSpan creates a child span. The returned context carries the new span IDs,
// so nested calls and outgoing HTTP requests propagate the same trace.
func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	parent, ok := TraceFromContext(ctx)
	if !ok {
		parent = TraceContext{TraceID: NewTraceID(), SpanID: "", Sampled: true}
	}
	tc := TraceContext{
		TraceID:      parent.TraceID,
		SpanID:       NewSpanID(),
		ParentSpanID: parent.SpanID,
		Sampled:      parent.Sampled,
	}
	ctx = WithTraceContext(ctx, tc)
	return ctx, &Span{Name: name, Trace: tc, Start: time.Now(), attrs: map[string]any{}}
}

// SetGlobalLogger and SetGlobalExporter are used by the HTTP middleware.
var globalLogger atomic.Pointer[Logger]
var globalExporter atomic.Pointer[spanExporter]

func SetGlobalLogger(l *Logger) { globalLogger.Store(l) }

func (s *Span) Set(k string, v any) {
	if s == nil {
		return
	}
	s.attrs[k] = v
}

func (s *Span) End(ctx context.Context) {
	if s == nil || !s.finished.CompareAndSwap(false, true) {
		return
	}
	duration := time.Since(s.Start)
	s.attrs["duration_ms"] = float64(duration.Microseconds()) / 1000.0
	if l := globalLogger.Load(); l != nil {
		kv := make([]any, 0, len(s.attrs)*2)
		for k, v := range s.attrs {
			kv = append(kv, k, v)
		}
		l.write(ctx, "info", "span", append(kv, "span_name", s.Name, "trace_id", s.Trace.TraceID,
			"span_id", s.Trace.SpanID, "parent_span_id", s.Trace.ParentSpanID)...)
	}
	if e := globalExporter.Load(); e != nil && s.Trace.Sampled {
		e.export(s)
	}
}

// ---------------------------------------------------------------------------
// Minimal OTLP/HTTP JSON exporter (optional)
// ---------------------------------------------------------------------------

type spanExporter struct {
	endpoint string
	service  string
	client   *http.Client
}

// EnableOTLP configures span export to an OTLP/HTTP endpoint such as
// http://otel-collector:4318. Empty endpoint disables export.
func EnableOTLP(endpoint, service string) {
	if endpoint == "" {
		return
	}
	globalExporter.Store(&spanExporter{
		endpoint: strings.TrimRight(endpoint, "/"),
		service:  service,
		client:   &http.Client{Timeout: 3 * time.Second},
	})
}

func (e *spanExporter) export(s *Span) {
	end := time.Now()
	body := map[string]any{
		"resourceSpans": []any{map[string]any{
			"resource": map[string]any{"attributes": []any{
				map[string]any{"key": "service.name", "value": map[string]any{"stringValue": e.service}},
			}},
			"scopeSpans": []any{map[string]any{
				"scope": map[string]any{"name": "bnpb-gateway"},
				"spans": []any{map[string]any{
					"traceId":           s.Trace.TraceID,
					"spanId":            s.Trace.SpanID,
					"parentSpanId":      s.Trace.ParentSpanID,
					"name":              s.Name,
					"kind":              1,
					"startTimeUnixNano": strconv.FormatInt(s.Start.UnixNano(), 10),
					"endTimeUnixNano":   strconv.FormatInt(end.UnixNano(), 10),
					"attributes":        otlpAttrs(s.attrs),
				}},
			}},
		}},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return
	}
	go func() {
		req, err := http.NewRequest(http.MethodPost, e.endpoint+"/v1/traces", strings.NewReader(string(raw)))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := e.client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
}

func otlpAttrs(attrs map[string]any) []any {
	out := make([]any, 0, len(attrs))
	for k, v := range attrs {
		switch value := v.(type) {
		case string:
			out = append(out, map[string]any{"key": k, "value": map[string]any{"stringValue": value}})
		case bool:
			out = append(out, map[string]any{"key": k, "value": map[string]any{"boolValue": value}})
		case int:
			out = append(out, map[string]any{"key": k, "value": map[string]any{"intValue": strconv.Itoa(value)}})
		case int64:
			out = append(out, map[string]any{"key": k, "value": map[string]any{"intValue": strconv.FormatInt(value, 10)}})
		case float64:
			out = append(out, map[string]any{"key": k, "value": map[string]any{"doubleValue": value}})
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// HTTP middleware
// ---------------------------------------------------------------------------

type HTTPMiddleware struct {
	Service  string
	Logger   *Logger
	Registry *Registry
	Requests *CounterVec
	Duration *HistogramVec
	InFlight *GaugeVec
	TraceOut bool
}

func NewHTTPMiddleware(service string, logger *Logger, registry *Registry) *HTTPMiddleware {
	return &HTTPMiddleware{
		Service:  service,
		Logger:   logger,
		Registry: registry,
		Requests: registry.Counter("http_requests_total", "HTTP requests by method, route and status",
			"service", "method", "route", "status", "role", "outcome"),
		Duration: registry.Histogram("http_request_duration_seconds", "HTTP request latency",
			[]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			"service", "method", "route"),
		InFlight: registry.Gauge("http_in_flight_requests", "In-flight HTTP requests", "service"),
		TraceOut: true,
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (m *HTTPMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corr := r.Header.Get("X-Correlation-ID")
		if corr == "" {
			corr = NewTraceID()
		}
		ctx := WithCorrelationID(r.Context(), corr)

		tc, ok := ParseTraceparent(r.Header.Get("Traceparent"))
		if !ok {
			tc = TraceContext{TraceID: NewTraceID(), SpanID: NewSpanID(), Sampled: true}
		}
		ctx = WithTraceContext(ctx, tc)

		ctx, span := StartSpan(ctx, r.Method+" "+r.URL.Path)
		defer span.End(ctx)
		span.Set("http.method", r.Method)
		span.Set("http.target", r.URL.Path)

		if m.TraceOut {
			w.Header().Set("Traceparent", NewTraceparent(mustTrace(ctx)))
			w.Header().Set("X-Correlation-ID", corr)
		}

		m.InFlight.WithLabelValues(m.Service).Add(1)
		defer m.InFlight.WithLabelValues(m.Service).Add(-1)

		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))
		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}
		route := routeLabel(r)
		role := r.Header.Get("X-Client-Role")
		if role == "" {
			role = "anonymous"
		}
		outcome := "success"
		if status >= 400 {
			outcome = "error"
		}
		if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
			outcome = "shed"
		}
		m.Requests.WithLabelValues(m.Service, r.Method, route, strconv.Itoa(status), role, outcome).Inc()
		m.Duration.WithLabelValues(m.Service, r.Method, route).Observe(time.Since(start).Seconds())

		if m.Logger != nil {
			m.Logger.Info(ctx, "http_request",
				"method", r.Method, "path", r.URL.Path, "route", route,
				"status", status, "latency_ms", time.Since(start).Milliseconds(),
				"remote_addr", r.RemoteAddr, "role", role, "outcome", outcome,
			)
		}
	})
}

func mustTrace(ctx context.Context) TraceContext {
	tc, _ := TraceFromContext(ctx)
	return tc
}

// InjectHeaders propagates correlation and W3C trace context on outgoing
// requests. It is the client-side counterpart of HTTPMiddleware.
func InjectHeaders(ctx context.Context, h http.Header) {
	if tc, ok := TraceFromContext(ctx); ok {
		h.Set("Traceparent", NewTraceparent(tc))
	}
	if id := CorrelationID(ctx); id != "" {
		h.Set("X-Correlation-ID", id)
	}
}

func routeLabel(r *http.Request) string {
	// Go 1.22+ populates PathValue on the routed request, but this middleware
	// wraps the mux, so normalize known high-cardinality paths here.
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, "/hazards/"):
		return "/hazards/{id}"
	case path == "":
		return "/"
	default:
		return path
	}
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// The standard math package is imported through these tiny wrappers so the file
// above stays readable and the atomic float pattern is in one place.
func mathFloat64bits(f float64) uint64     { return math.Float64bits(f) }
func mathFloat64frombits(b uint64) float64 { return math.Float64frombits(b) }
