package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rizalkhairan/distributed-system/observability"
)

func main() {
	path := env("STATE_PATH", filepath.Join("data", "coordinator.json"))
	addr := env("ADDR", ":8080")
	token := os.Getenv("COORDINATOR_TOKEN")

	var initial persisted
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &initial); err != nil {
			log.Fatalf("load coordinator state: %v", err)
		}
	} else if !os.IsNotExist(err) {
		log.Fatalf("read coordinator state: %v", err)
	}

	manager := NewManager(wallClock{}, saveState(path))
	manager.Restore(initial)

	logger := obs.NewLogger("coordinator")
	obs.SetGlobalLogger(logger)
	obs.EnableOTLP(env("OTEL_EXPORTER_OTLP_ENDPOINT", ""), "coordinator")
	registry := obs.NewRegistry("coordinator")
	middleware := obs.NewHTTPMiddleware("coordinator", logger, registry)

	holderGauge := registry.Gauge("coordinator_lease_holder", "Current lease holder epoch (0 when idle)", "source", "holder")
	transitions := registry.Counter("coordinator_lease_transitions_total", "Lease transitions", "source", "event")
	commits := registry.Counter("coordinator_checkpoint_commits_total", "Checkpoint CAS results", "source", "result")
	outboxGauge := registry.Gauge("coordinator_outbox_entries", "Outbox entries by status", "status")
	fenced := registry.Counter("coordinator_fenced_writes_total", "Rejected stale writes", "operation")

	manager.OnLeaseTransition = func(source, holder, event string, epoch int64) {
		transitions.WithLabelValues(source, event).Inc()
		value := float64(epoch)
		if event == "released" || event == "expired" {
			value = 0
		}
		holderGauge.WithLabelValues(source, holder).Set(value)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", registry.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "store": "local-disk"})
	})

	auth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if token != "" && r.Header.Get("X-Internal-Key") != token {
				writeError(w, http.StatusUnauthorized, "unauthorized", "invalid coordinator token")
				return
			}
			next(w, r)
		}
	}

	mux.HandleFunc("POST /v1/leases/{source}/acquire", auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Holder string `json:"holder"`
			TTLMs  int64  `json:"ttl_ms"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil || body.Holder == "" {
			writeError(w, http.StatusBadRequest, "invalid_body", "holder is required")
			return
		}
		ttl := durationOrDefault(body.TTLMs, 15*time.Second)
		lease, err := manager.Acquire(r.PathValue("source"), body.Holder, ttl)
		if err == ErrLeaseHeld {
			writeError(w, http.StatusConflict, "lease_held", "lease is held by another holder")
			return
		}
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "store_error", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, leaseJSON(lease, manager.clock))
	}))

	mux.HandleFunc("POST /v1/leases/{source}/renew", auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Holder string `json:"holder"`
			Epoch  int64  `json:"epoch"`
			TTLMs  int64  `json:"ttl_ms"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
			return
		}
		lease, err := manager.Renew(r.PathValue("source"), body.Holder, body.Epoch, durationOrDefault(body.TTLMs, 15*time.Second))
		if err != nil {
			writeError(w, http.StatusConflict, "stale_epoch", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, leaseJSON(lease, manager.clock))
	}))

	mux.HandleFunc("POST /v1/leases/{source}/release", auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Holder string `json:"holder"`
			Epoch  int64  `json:"epoch"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
			return
		}
		if err := manager.Release(r.PathValue("source"), body.Holder, body.Epoch); err != nil {
			writeError(w, http.StatusServiceUnavailable, "store_error", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"released": true})
	}))

	mux.HandleFunc("GET /v1/leases/{source}", auth(func(w http.ResponseWriter, r *http.Request) {
		lease, active := manager.Lease(r.PathValue("source"))
		response := leaseJSON(lease, manager.clock)
		response["active"] = active
		writeJSON(w, http.StatusOK, response)
	}))

	mux.HandleFunc("GET /v1/checkpoints/{source}", auth(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, manager.Checkpoint(r.PathValue("source")))
	}))

	mux.HandleFunc("PUT /v1/checkpoints/{source}", auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Epoch  int64  `json:"epoch"`
			Cursor string `json:"cursor"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
			return
		}
		source := r.PathValue("source")
		checkpoint, err := manager.CommitCheckpoint(source, body.Epoch, body.Cursor)
		if err != nil {
			commits.WithLabelValues(source, "fenced").Inc()
			fenced.WithLabelValues("checkpoint").Inc()
			writeError(w, http.StatusConflict, "stale_epoch", err.Error())
			return
		}
		commits.WithLabelValues(source, "committed").Inc()
		writeJSON(w, http.StatusOK, checkpoint)
	}))

	mux.HandleFunc("POST /v1/outbox", auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			DedupeKey string         `json:"dedupe_key"`
			Payload   map[string]any `json:"payload"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&body); err != nil || body.DedupeKey == "" || body.Payload == nil {
			writeError(w, http.StatusBadRequest, "invalid_body", "dedupe_key and payload are required")
			return
		}
		entry, created, err := manager.Enqueue(body.DedupeKey, body.Payload)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "store_error", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"entry": entry, "created": created})
	}))

	mux.HandleFunc("GET /v1/outbox", auth(func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		writeJSON(w, http.StatusOK, manager.Outbox(r.URL.Query().Get("status"), limit))
	}))

	mux.HandleFunc("POST /v1/outbox/claim", auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Holder  string `json:"holder"`
			Limit   int    `json:"limit"`
			LeaseMs int64  `json:"lease_ms"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil || body.Holder == "" {
			writeError(w, http.StatusBadRequest, "invalid_body", "holder is required")
			return
		}
		entries := manager.Claim(body.Holder, body.Limit, durationOrDefault(body.LeaseMs, 15*time.Second))
		refreshOutboxGauge(outboxGauge, manager)
		writeJSON(w, http.StatusOK, entries)
	}))

	mux.HandleFunc("POST /v1/outbox/{id}/ack", auth(func(w http.ResponseWriter, r *http.Request) {
		if err := manager.Ack(r.PathValue("id"), ""); err != nil {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}
		refreshOutboxGauge(outboxGauge, manager)
		writeJSON(w, http.StatusOK, map[string]any{"acked": true})
	}))

	mux.HandleFunc("POST /v1/outbox/{id}/nack", auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Reason       string `json:"reason"`
			RetryAfterMs int64  `json:"retry_after_ms"`
		}
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body)
		if err := manager.Nack(r.PathValue("id"), "", body.Reason, durationOrDefault(body.RetryAfterMs, time.Second)); err != nil {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}
		refreshOutboxGauge(outboxGauge, manager)
		writeJSON(w, http.StatusOK, map[string]any{"released": true})
	}))

	mux.HandleFunc("GET /v1/outbox/stats", auth(func(w http.ResponseWriter, r *http.Request) {
		stats := manager.OutboxStats()
		for status, count := range stats {
			outboxGauge.WithLabelValues(status).Set(float64(count))
		}
		writeJSON(w, http.StatusOK, stats)
	}))

	logger.Info(context.Background(), "coordinator_started", "addr", addr, "state_path", path)
	server := &http.Server{Addr: addr, Handler: middleware.Wrap(mux),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error(context.Background(), "server_failed", "error", err.Error())
	}
}

func refreshOutboxGauge(gauge *obs.GaugeVec, manager *Manager) {
	for status, count := range manager.OutboxStats() {
		gauge.WithLabelValues(status).Set(float64(count))
	}
}

func leaseJSON(lease leaseState, clock Clock) map[string]any {
	remaining := lease.ExpiresMono.Sub(clock.Mono())
	if remaining < 0 {
		remaining = 0
	}
	return map[string]any{
		"source": lease.Source, "holder": lease.Holder, "epoch": lease.Epoch,
		"active": true, "expires_in_ms": remaining.Milliseconds(),
	}
}

func durationOrDefault(ms int64, fallback time.Duration) time.Duration {
	if ms <= 0 {
		return fallback
	}
	d := time.Duration(ms) * time.Millisecond
	if d < time.Second {
		return time.Second
	}
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}

func saveState(path string) func(persisted) error {
	return func(state persisted) error {
		raw, err := json.MarshalIndent(state, "", " ")
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, raw, 0o644); err != nil {
			return err
		}
		return os.Rename(tmp, path)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
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
