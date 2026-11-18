package auth

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/rizalkhairan/distributed-system/shared/telemetry"
	"go.opentelemetry.io/otel/trace"
)

func TestProxyUsesRequestContextForLoginAndRefresh(t *testing.T) {
	rt, err := telemetry.New(telemetry.Config{Service: "client-api", SampleRatio: 1, Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Flush()
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(telemetry.WithCorrelation(context.Background(), "auth-context"), sc)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Correlation-ID") != "auth-context" || r.Header.Get("traceparent") == "" {
			t.Error("request context not propagated")
		}
		io.WriteString(w, `{"data":{}}`)
	}))
	defer server.Close()
	proxy := NewAuthProxy(server.URL, rt)
	if _, err := proxy.LoginContext(ctx, "test-user", "test-password"); err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.RefreshContext(ctx, "test-refresh"); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := proxy.LoginContext(canceled, "test-user", "test-password"); err == nil {
		t.Fatal("cancellation ignored")
	}
	if calls.Load() != 2 {
		t.Fatalf("unexpected upstream calls: %d", calls.Load())
	}
}
