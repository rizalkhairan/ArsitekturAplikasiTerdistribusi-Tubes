package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rizalkhairan/distributed-system/observability"
)

func serve(store *Store, addr string) {
	logger := obs.NewLogger("canonical-store")
	obs.SetGlobalLogger(logger)
	obs.EnableOTLP(env("OTEL_EXPORTER_OTLP_ENDPOINT", ""), "canonical-store")
	registry := obs.NewRegistry("canonical-store")
	middleware := obs.NewHTTPMiddleware("canonical-store", logger, registry)

	upserts := registry.Counter("storage_upserts_total", "Canonical mutations by result", "result")
	fenced := registry.Counter("storage_fenced_writes_total", "Writes rejected by fencing", "scope")
	eventsGauge := registry.Gauge("storage_events", "Active canonical events", "source")
	writeToken := env("WRITE_TOKEN", "")
	auditToken := env("AUDIT_TOKEN", "")

	authAny := func(tokens []string, next http.HandlerFunc) http.HandlerFunc {
		allowed := false
		for _, token := range tokens {
			if token != "" {
				allowed = true
			}
		}
		if !allowed {
			return next
		}
		return func(w http.ResponseWriter, r *http.Request) {
			got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			for _, token := range tokens {
				if token != "" && got == token {
					next(w, r)
					return
				}
			}
			writeError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing bearer token")
		}
	}
	auth := func(token string, next http.HandlerFunc) http.HandlerFunc {
		return authAny([]string{token}, next)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", registry.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "store": "local-disk"})
	})

	mux.HandleFunc("PUT /internal/canonical/events/{source}/{ref}", auth(writeToken, func(w http.ResponseWriter, r *http.Request) {
		handleMutation(w, r, store, upserts, fenced, logger, false)
	}))
	mux.HandleFunc("PUT /internal/admin/events/{source}/{ref}", auth(writeToken, func(w http.ResponseWriter, r *http.Request) {
		handleMutation(w, r, store, upserts, fenced, logger, true)
	}))
	mux.HandleFunc("GET /internal/canonical/events", auth(writeToken, func(w http.ResponseWriter, r *http.Request) {
		handleQuery(w, r, store)
	}))
	mux.HandleFunc("GET /internal/audit/events", authAny([]string{auditToken, writeToken}, func(w http.ResponseWriter, r *http.Request) {
		handleQuery(w, r, store)
	}))
	mux.HandleFunc("GET /internal/audit/checksum", authAny([]string{auditToken, writeToken}, func(w http.ResponseWriter, r *http.Request) {
		source := r.URL.Query().Get("source")
		count, checksum := store.Checksum(source)
		var sourceValue any
		if source != "" {
			sourceValue = source
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"source": sourceValue, "count": count, "checksum": checksum,
			"algorithm": "sha256", "as_of": time.Now().UTC().Format(time.RFC3339Nano),
			"partial": false, "unavailable_units": []string{},
		})
	}))
	mux.HandleFunc("GET /internal/audit/location", authAny([]string{auditToken, writeToken}, func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("hazard_id")
		if id == "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "hazard_id is required")
			return
		}
		event, ok := store.Location(id)
		if !ok {
			writeError(w, http.StatusNotFound, "not_found", "unknown hazard_id")
			return
		}
		location := store.location()
		writeJSON(w, http.StatusOK, map[string]any{
			"hazard_id": id,
			"locations": []any{map[string]any{
				"node": location["node"], "unit": location["unit"], "role": "primary",
			}},
			"partial": false,
			"version": event.Version,
		})
	}))

	// Legacy helpers for the Aggregator read cache and duplicate detector.
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		stored := store.LegacyList(r.URL.Query().Get("source"), r.URL.Query().Get("since"), limit)
		events := make([]HazardEvent, 0, len(stored))
		for _, item := range stored {
			events = append(events, item.Event)
		}
		writeJSON(w, http.StatusOK, events)
	})
	mux.HandleFunc("GET /events/{ref}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		for _, event := range store.LegacyList("", "", 0) {
			if event.Event.SourceRefID == ref {
				writeJSON(w, http.StatusOK, event.Event)
				return
			}
		}
		writeError(w, http.StatusNotFound, "not_found", "event not found")
	})
	mux.HandleFunc("GET /events/hazard/{id}", func(w http.ResponseWriter, r *http.Request) {
		event, ok := store.Location(r.PathValue("id"))
		if !ok {
			writeError(w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeJSON(w, http.StatusOK, event.Event)
	})
	mux.HandleFunc("GET /admin/requests", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, store.Requests())
	})
	mux.HandleFunc("GET /admin/stats", func(w http.ResponseWriter, r *http.Request) {
		counts := store.Counts()
		for source, count := range counts {
			eventsGauge.WithLabelValues(source).Set(float64(count))
		}
		writeJSON(w, http.StatusOK, map[string]any{"counts": counts})
	})

	server := &http.Server{Addr: addr, Handler: middleware.Wrap(mux),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error(context.Background(), "server_failed", "error", err.Error())
	}
}

type mutationBody struct {
	Version   int64  `json:"version"`
	Operation string `json:"operation"`
	Authority struct {
		Partition string `json:"partition"`
		Epoch     int64  `json:"epoch"`
	} `json:"authority"`
	CorrelationID string       `json:"correlation_id"`
	Event         *HazardEvent `json:"event"`
}

func handleMutation(w http.ResponseWriter, r *http.Request, store *Store, upserts *obs.CounterVec,
	fenced *obs.CounterVec, logger *obs.Logger, admin bool) {
	var body mutationBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	source := r.PathValue("source")
	ref := r.PathValue("ref")
	caller := r.Header.Get("X-Caller-Instance")
	if caller == "" {
		caller = "unknown"
	}
	var stored storedEvent
	var created, replayed bool
	var err error
	if admin {
		stored, created, replayed, err = store.MutateAdmin(source, ref, body.Version, body.Operation, body.Event)
	} else {
		stored, created, replayed, err = store.Mutate(source, ref, body.Version, body.Operation,
			body.Authority.Partition, body.Authority.Epoch, body.Event)
	}
	result := "stored"
	status := http.StatusOK
	switch {
	case errors.Is(err, ErrFenced):
		result, status = "fenced", http.StatusConflict
		fenced.WithLabelValues(source).Inc()
		writeError(w, status, "fenced", err.Error())
	case errors.Is(err, ErrStaleVersion):
		result, status = "stale_version", http.StatusConflict
		writeError(w, status, "stale_version", err.Error())
	case errors.Is(err, ErrVersionConflict):
		result, status = "version_conflict", http.StatusConflict
		writeError(w, status, "version_conflict", err.Error())
	case err != nil:
		result, status = "error", http.StatusBadRequest
		writeError(w, status, "invalid_event", err.Error())
	case created:
		result, status = "created", http.StatusCreated
	}
	upserts.WithLabelValues(result).Inc()
	store.recordRequest(RequestLog{TS: time.Now().UTC().Format(time.RFC3339Nano), CallerInstance: caller,
		Source: source, SourceRefID: ref, Epoch: body.Authority.Epoch, Version: body.Version,
		Operation: body.Operation, Result: result})
	logger.Info(r.Context(), "canonical_mutation", "caller_instance", caller, "source", source,
		"source_ref_id", ref, "epoch", body.Authority.Epoch, "version", body.Version,
		"operation", body.Operation, "result", result)
	if err != nil {
		return
	}
	writeJSON(w, status, map[string]any{
		"hazard_id": stored.Event.HazardID, "version": stored.Version, "operation": stored.Operation,
		"replayed": replayed, "durability": "local-disk",
	})
}

func handleQuery(w http.ResponseWriter, r *http.Request, store *Store) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_cursor", err.Error())
		return
	}
	result := store.Query(Query{Source: q.Get("source"), Since: q.Get("since"), Limit: limit, Offset: offset})
	items := make([]HazardEvent, 0, len(result.Items))
	for _, item := range result.Items {
		items = append(items, item.Event)
	}
	var next any
	if result.Next >= 0 {
		next = encodeCursor(result.Next)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items, "next_cursor": next, "partial": false, "unavailable_units": []string{},
	})
}

func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte("v1:" + strconv.Itoa(offset)))
}

func decodeCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || !strings.HasPrefix(string(raw), "v1:") {
		return 0, errors.New("invalid cursor")
	}
	return strconv.Atoi(strings.TrimPrefix(string(raw), "v1:"))
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
