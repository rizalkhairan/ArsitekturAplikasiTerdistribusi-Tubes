// telemetry-example is a two-process HTTP instrumentation example, not a product service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rizalkhairan/distributed-system/shared/telemetry"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	t, err := telemetry.FromEnv(ctx)
	if err != nil {
		return err
	}
	defer func() {
		flush, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := t.Shutdown(flush); err != nil {
			t.Logger.Error("trace_shutdown", "error", err)
		}
	}()
	client := &http.Client{Transport: t.Transport(nil), Timeout: 3 * time.Second}
	mux := http.NewServeMux()
	mux.Handle("GET /work", t.Handler("GET /work", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := t.Tracer.Start(r.Context(), "example.compute")
		defer span.End()
		t.Log(ctx).Info("work_complete")
		fmt.Fprintln(w, "work complete")
	})))
	mux.Handle("GET /demo", t.Handler("GET /demo", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream := os.Getenv("EXAMPLE_UPSTREAM_URL")
		if upstream == "" {
			http.Error(w, "Set EXAMPLE_UPSTREAM_URL", http.StatusServiceUnavailable)
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream+"/work", nil)
		if err != nil {
			http.Error(w, "Invalid upstream", http.StatusServiceUnavailable)
			return
		}
		response, err := client.Do(req)
		if err != nil {
			t.Log(r.Context()).Warn("upstream_unavailable")
			http.Error(w, "Upstream unavailable", 503)
			return
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			http.Error(w, "Upstream failed", 503)
			return
		}
		fmt.Fprintln(w, "request completed across two services")
	})))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	mux.Handle("/", t.Handler("unmatched", http.NotFoundHandler()))
	metrics := http.NewServeMux()
	metrics.Handle("GET /metrics", t.MetricsHandler())
	addr, metricsAddr := os.Getenv("EXAMPLE_ADDR"), os.Getenv("METRICS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8096"
	}
	if metricsAddr == "" {
		metricsAddr = "127.0.0.1:9096"
	}
	app := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	admin := &http.Server{Addr: metricsAddr, Handler: metrics, ReadHeaderTimeout: 5 * time.Second}
	errorsCh := make(chan error, 2)
	go func() { errorsCh <- app.ListenAndServe() }()
	go func() { errorsCh <- admin.ListenAndServe() }()
	t.Logger.Info("example_started", "address", addr, "metrics_address", metricsAddr)
	select {
	case <-ctx.Done():
	case err = <-errorsCh:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return errors.Join(ignoreClosed(err), app.Shutdown(shutdown), admin.Shutdown(shutdown))
}

func ignoreClosed(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
