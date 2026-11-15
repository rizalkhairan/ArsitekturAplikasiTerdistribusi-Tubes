package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func runtimeForTest(t *testing.T, service string, exp *tracetest.InMemoryExporter, output io.Writer) *Runtime {
	t.Helper()
	if output == nil {
		output = io.Discard
	}
	rt, err := New(Config{Service: service, Exporter: exp, SampleRatio: 1, Output: output})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := rt.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return rt
}

func TestHTTPTraceParentageAndCorrelation(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	front := runtimeForTest(t, "front", exp, nil)
	back := runtimeForTest(t, "back", exp, nil)
	seen := make(chan string, 1)
	server := httptest.NewServer(back.Handler("GET /work", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- CorrelationID(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})))
	defer server.Close()
	client := &http.Client{Transport: front.Transport(nil), Timeout: 3 * time.Second}
	var original *http.Request
	h := front.Handler("GET /demo", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		original, err = http.NewRequestWithContext(r.Context(), "GET", server.URL+"/work", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(original)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
	}))
	request := httptest.NewRequest("GET", "/demo", nil)
	request.Header.Set(CorrelationHeader, "request-42")
	request.Header.Set("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request)
	if w.Code != 204 || w.Header().Get(CorrelationHeader) != "request-42" || <-seen != "request-42" {
		t.Fatal("status or correlation ID was lost")
	}
	if len(original.Header) != 0 {
		t.Fatal("transport mutated caller's request headers")
	}
	for _, rt := range []*Runtime{front, back} {
		if err := rt.Provider.ForceFlush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	spans := exp.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("want server/client/server spans, got %d", len(spans))
	}
	var frontSpan, clientSpan, backSpan tracetest.SpanStub
	for _, s := range spans {
		if s.SpanContext.TraceID().String() != "0123456789abcdef0123456789abcdef" {
			t.Fatal("trace fragmented across HTTP")
		}
		switch {
		case s.Name == "GET /demo":
			frontSpan = s
		case s.Name == "GET /work":
			backSpan = s
		case s.SpanKind == trace.SpanKindClient:
			clientSpan = s
		}
	}
	if frontSpan.Parent.SpanID().String() != "0123456789abcdef" || clientSpan.Parent.SpanID() != frontSpan.SpanContext.SpanID() || backSpan.Parent.SpanID() != clientSpan.SpanContext.SpanID() {
		t.Fatal("incorrect cross-service span parentage")
	}
}

func TestAsyncCarrierAndJSONLog(t *testing.T) {
	var output bytes.Buffer
	rt := runtimeForTest(t, "worker", tracetest.NewInMemoryExporter(), &output)
	ctx := WithCorrelation(context.Background(), "durable-message-1")
	ctx, producer := rt.Tracer.Start(ctx, "publish", trace.WithSpanKind(trace.SpanKindProducer))
	carrier := propagation.MapCarrier{}
	rt.Inject(ctx, carrier)
	serialized, _ := json.Marshal(carrier)
	var received propagation.MapCarrier
	if err := json.Unmarshal(serialized, &received); err != nil {
		t.Fatal(err)
	}
	ctx = rt.Extract(context.Background(), received)
	ctx, consumer := rt.Tracer.Start(ctx, "consume", trace.WithSpanKind(trace.SpanKindConsumer))
	rt.Log(ctx).Info("processed")
	consumer.End()
	producer.End()
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["correlation_id"] != "durable-message-1" || entry["service"] != "worker" || entry["trace_id"] != producer.SpanContext().TraceID().String() || entry["span_id"] != consumer.SpanContext().SpanID().String() {
		t.Fatalf("missing or wrong structured context: %s", output.String())
	}
	if err := rt.Provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if producer.SpanContext().SpanID() == consumer.SpanContext().SpanID() {
		t.Fatal("consumer reused producer span")
	}
}

func TestStatusMetricsAndStreaming(t *testing.T) {
	rt := runtimeForTest(t, "api", tracetest.NewInMemoryExporter(), nil)
	write := rt.Handler("GET /events/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "flush") {
			w.WriteHeader(201)
			w.WriteHeader(503) // ignored by net/http; must not change observed status
		} else {
			w.(http.Flusher).Flush()
		}
		_, _ = w.Write([]byte("ok"))
	}))
	for _, path := range []string{"/events/private-id", "/events/flush"} {
		write.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
	w := httptest.NewRecorder()
	rt.MetricsHandler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	metrics := w.Body.String()
	for _, expected := range []string{
		`bnpb_http_requests_total{method="GET",route="GET /events/{id}",service="api",status_code="201"} 1`,
		`bnpb_http_requests_total{method="GET",route="GET /events/{id}",service="api",status_code="200"} 1`,
		`bnpb_http_requests_in_flight{service="api"} 0`,
		`bnpb_http_request_duration_seconds_count{method="GET",route="GET /events/{id}",service="api"} 2`,
	} {
		if !strings.Contains(metrics, expected) {
			t.Errorf("missing metric %s", expected)
		}
	}
	if strings.Contains(metrics, "private-id") || strings.Contains(metrics, `status_code="503"`) {
		t.Fatal("unbounded label or incorrect status")
	}
}

func TestInvalidCorrelationAndTraceContext(t *testing.T) {
	rt := runtimeForTest(t, "api", tracetest.NewInMemoryExporter(), nil)
	for _, id := range []string{"", "invalid\nheader", strings.Repeat("x", 129)} {
		ctx := rt.Extract(context.Background(), propagation.MapCarrier{"x-correlation-id": id, "traceparent": "invalid"})
		if !validCorrelation(CorrelationID(ctx)) || len(CorrelationID(ctx)) != 32 {
			t.Fatal("invalid ID not replaced")
		}
		if trace.SpanContextFromContext(ctx).IsValid() {
			t.Fatal("accepted malformed parent")
		}
	}
	if _, err := New(Config{Service: "test", SampleRatio: math.NaN()}); err == nil {
		t.Fatal("accepted NaN sampling")
	}
}

func TestPanicRecordsFailureAndPropagates(t *testing.T) {
	rt := runtimeForTest(t, "api", tracetest.NewInMemoryExporter(), nil)
	h := rt.Handler("GET /panic", http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("test") }))
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic was swallowed")
			}
		}()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/panic", nil))
	}()
	w := httptest.NewRecorder()
	rt.MetricsHandler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), `status_code="500"} 1`) {
		t.Fatal("panic not counted as failure")
	}
}
