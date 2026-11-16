// Field-team notifier: dedupes critical alerts and records end-to-end latency.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rizalkhairan/distributed-system/observability"
)

type Alert struct {
	DedupeKey    string         `json:"dedupe_key"`
	EventID      string         `json:"event_id"`
	Source       string         `json:"source"`
	StationID    string         `json:"station_id"`
	ObservedAt   string         `json:"observed_at"`
	DispatchedAt string         `json:"dispatched_at"`
	Severity     string         `json:"severity"`
	Message      string         `json:"message"`
	Attributes   map[string]any `json:"attributes"`
}

type received struct {
	Alert
	ReceivedAt string  `json:"received_at"`
	E2EMs      float64 `json:"e2e_ms"`
	PipelineMs float64 `json:"pipeline_ms"`
}

type receiver struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	items []received
	max   int
	dupes uint64
}

func newReceiver(max int) *receiver {
	return &receiver{seen: map[string]struct{}{}, max: max}
}

func (r *receiver) accept(alert Alert) (received, bool) {
	now := time.Now().UTC()
	key := alert.DedupeKey
	if key == "" {
		key = strings.Join([]string{alert.Source, alert.StationID, alert.ObservedAt, alert.EventID}, "|")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.seen[key]; ok {
		r.dupes++
		return received{}, false
	}
	r.seen[key] = struct{}{}
	item := received{Alert: alert, ReceivedAt: now.Format(time.RFC3339Nano)}
	if observed, err := time.Parse(time.RFC3339Nano, alert.ObservedAt); err == nil {
		item.E2EMs = float64(now.Sub(observed).Microseconds()) / 1000.0
		if item.E2EMs < 0 {
			item.E2EMs = 0
		}
	}
	if dispatched, err := time.Parse(time.RFC3339Nano, alert.DispatchedAt); err == nil {
		item.PipelineMs = float64(now.Sub(dispatched).Microseconds()) / 1000.0
		if item.PipelineMs < 0 {
			item.PipelineMs = 0
		}
	}
	r.items = append(r.items, item)
	if len(r.items) > r.max {
		r.items = r.items[len(r.items)-r.max:]
	}
	return item, true
}

func (r *receiver) snapshot(limit int) []received {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]received, len(r.items))
	copy(out, r.items)
	sort.Slice(out, func(i, j int) bool { return out[i].ReceivedAt > out[j].ReceivedAt })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func main() {
	addr := env("ADDR", ":8080")
	token := os.Getenv("NOTIFIER_TOKEN")
	r := newReceiver(20000)

	logger := obs.NewLogger("field-notifier")
	obs.SetGlobalLogger(logger)
	registry := obs.NewRegistry("field-notifier")
	middleware := obs.NewHTTPMiddleware("field-notifier", logger, registry)
	alerts := registry.Counter("field_notifier_alerts_total", "Alerts by result and severity", "result", "severity")
	latency := registry.Histogram("critical_alert_end_to_end_seconds", "Critical alert observed_at to receiver",
		[]float64{0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 13, 30, 60}, "severity")
	pipeline := registry.Histogram("critical_alert_pipeline_seconds", "Critical alert dispatched_at to receiver (the part B controls)",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 13}, "severity")
	receivedGauge := registry.Gauge("field_notifier_received", "Alerts held in memory", "severity")

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", registry.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("POST /notify", func(w http.ResponseWriter, req *http.Request) {
		if token != "" && req.Header.Get("Authorization") != "Bearer "+token {
			writeError(w, http.StatusUnauthorized, "unauthorized", "invalid notifier token")
			return
		}
		var alert Alert
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, 1<<20)).Decode(&alert); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
			return
		}
		if alert.ObservedAt == "" {
			writeError(w, http.StatusBadRequest, "invalid_alert", "observed_at is required")
			return
		}
		item, first := r.accept(alert)
		if !first {
			alerts.WithLabelValues("duplicate", alert.Severity).Inc()
			writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate"})
			return
		}
		alerts.WithLabelValues("accepted", alert.Severity).Inc()
		if item.E2EMs > 0 {
			latency.WithLabelValues(alert.Severity).Observe(item.E2EMs / 1000.0)
		}
		if alert.DispatchedAt != "" {
			pipeline.WithLabelValues(alert.Severity).Observe(item.PipelineMs / 1000.0)
		}
		receivedGauge.WithLabelValues(alert.Severity).Set(float64(len(r.snapshot(0))))
		logger.Info(req.Context(), "alert_received", "dedupe_key", alert.DedupeKey, "event_id", alert.EventID,
			"station_id", alert.StationID, "severity", alert.Severity, "e2e_ms", item.E2EMs)
		writeJSON(w, http.StatusOK, map[string]any{"status": "accepted", "received_at": item.ReceivedAt, "e2e_ms": item.E2EMs})
	})
	mux.HandleFunc("GET /received", func(w http.ResponseWriter, req *http.Request) {
		limit, _ := strconv.Atoi(req.URL.Query().Get("limit"))
		writeJSON(w, http.StatusOK, map[string]any{"items": r.snapshot(limit), "duplicates": r.dupes})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, req *http.Request) {
		items := r.snapshot(0)
		var max, sum float64
		var maxPipeline, sumPipeline float64
		for _, item := range items {
			if item.E2EMs > max {
				max = item.E2EMs
			}
			sum += item.E2EMs
			if item.PipelineMs > maxPipeline {
				maxPipeline = item.PipelineMs
			}
			sumPipeline += item.PipelineMs
		}
		avg := 0.0
		if len(items) > 0 {
			avg = sum / float64(len(items))
		}
		avgPipeline := 0.0
		if len(items) > 0 {
			avgPipeline = sumPipeline / float64(len(items))
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"received": len(items), "duplicates": r.dupes,
			"max_e2e_ms": max, "avg_e2e_ms": avg,
			"max_pipeline_ms": maxPipeline, "avg_pipeline_ms": avgPipeline,
		})
	})

	server := &http.Server{Addr: addr, Handler: middleware.Wrap(mux),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error(context.Background(), "server_failed", "error", err.Error())
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
