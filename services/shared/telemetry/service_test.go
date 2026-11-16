package telemetry

import (
	"context"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMuxPreservesPathValuesAndCountsRejectedRequests(t *testing.T) {
	rt := runtimeForTest(t, "mux", tracetest.NewInMemoryExporter(), nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /hazards/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "event-123" {
			t.Errorf("lost path value: %q", r.PathValue("id"))
		}
		if CorrelationID(r.Context()) != "mux-test" {
			t.Error("lost correlation")
		}
		w.WriteHeader(204)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	limiter := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Reject") != "" {
				w.WriteHeader(429)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	handler := rt.Mux(mux, limiter)
	for _, test := range []struct {
		path   string
		reject bool
		status int
	}{{"/hazards/event-123", false, 204}, {"/hazards/event-123", true, 429}, {"/secret-unbounded-id", false, 404}, {"/health", false, 200}} {
		req := httptest.NewRequest("GET", test.path, nil)
		req.Header.Set(CorrelationHeader, "mux-test")
		if test.reject {
			req.Header.Set("Reject", "yes")
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != test.status {
			t.Fatalf("status %d", response.Code)
		}
	}
	response := httptest.NewRecorder()
	rt.MetricsHandler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	metrics := response.Body.String()
	for _, needle := range []string{`route="GET /hazards/{id}"`, `status_code="429"`, `route="unmatched"`} {
		if !strings.Contains(metrics, needle) {
			t.Errorf("missing %s", needle)
		}
	}
	for _, needle := range []string{"event-123", "secret-unbounded-id", `route="GET /health"`} {
		if strings.Contains(metrics, needle) {
			t.Errorf("unbounded/probe metric %s", needle)
		}
	}
	_ = rt.Provider.ForceFlush(context.Background())
}
