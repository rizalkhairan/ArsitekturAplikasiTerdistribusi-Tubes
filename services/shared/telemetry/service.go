package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// StartService configures telemetry and a separate management listener. Cancel ctx
// to stop that listener; call Shutdown with a fresh context to flush trace batches.
func StartService(ctx context.Context, service string) (*Runtime, error) {
	t, err := fromEnv(ctx, service)
	if err != nil {
		return nil, err
	}
	addr := os.Getenv("METRICS_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", t.MetricsHandler())
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Logger.Error("metrics_listener_failed", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	return t, nil
}

// Mux observes bounded ServeMux patterns, including rejections by middleware.
// Calling ServeHTTP on the mux preserves PathValue handling. Health probes stay
// outside request metrics and tracing, as does the separate metrics listener.
func (t *Runtime) Mux(mux *http.ServeMux, middleware ...func(http.Handler) http.Handler) http.Handler {
	var next http.Handler = mux
	for i := len(middleware) - 1; i >= 0; i-- {
		next = middleware[i](next)
	}
	var handlers sync.Map
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, route := mux.Handler(r)
		if route == "GET /health" || route == "GET /healthz" {
			next.ServeHTTP(w, r)
			return
		}
		if route == "" {
			route = "unmatched"
		}
		handler, ok := handlers.Load(route)
		if !ok {
			handler, _ = handlers.LoadOrStore(route, t.Handler(route, next))
		}
		handler.(http.Handler).ServeHTTP(w, r)
	})
}

// Flush logs exporter errors without affecting application shutdown.
func (t *Runtime) Flush() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := t.Shutdown(ctx); err != nil {
		t.Logger.Warn("trace_shutdown_failed", "error", fmt.Sprint(err))
	}
}
