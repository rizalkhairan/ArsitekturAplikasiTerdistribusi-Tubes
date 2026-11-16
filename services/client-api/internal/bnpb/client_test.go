package bnpb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rizalkhairan/distributed-system/shared/telemetry"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestCanonicalClientPreservesTraceAndCorrelation(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	front, err := telemetry.New(telemetry.Config{Service: "client-api", Exporter: exp, SampleRatio: 1, Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer front.Flush()
	back, err := telemetry.New(telemetry.Config{Service: "bnpb", Exporter: exp, SampleRatio: 1, Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer back.Flush()
	upstream := httptest.NewServer(back.Handler("GET /hazards", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Key") != "test-internal" {
			t.Error("internal authentication lost")
		}
		if telemetry.CorrelationID(r.Context()) != "request-test" {
			t.Error("correlation lost")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, "[]")
	})))
	defer upstream.Close()
	client := NewClient(upstream.URL, time.Second, "test-internal", front)
	handler := front.Handler("GET /hazards", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, err := client.ListHazards(r.Context(), "BMKG", "", "", 1); err != nil {
			t.Error(err)
			w.WriteHeader(502)
		}
	}))
	req := httptest.NewRequest("GET", "/hazards", nil)
	req.Header.Set("X-Correlation-ID", "request-test")
	handler.ServeHTTP(httptest.NewRecorder(), req)
	_ = front.Provider.ForceFlush(context.Background())
	_ = back.Provider.ForceFlush(context.Background())
	spans := exp.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("want server/client/server spans, got %d", len(spans))
	}
	counts := map[trace.SpanKind]int{}
	for _, s := range spans {
		counts[s.SpanKind]++
		if s.SpanContext.TraceID() != spans[0].SpanContext.TraceID() {
			t.Error("fragmented trace")
		}
	}
	if counts[trace.SpanKindServer] != 2 || counts[trace.SpanKindClient] != 1 {
		t.Fatal("missing hop spans")
	}
}
