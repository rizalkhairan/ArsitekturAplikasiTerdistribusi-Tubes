// canonical-api exposes the legacy canonical-store HTTP shape on top of the
// platform PostgreSQL/Citus canonical store. It is the compatibility bridge
// used while the Aggregator still speaks the original store protocol.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/canonical"
)

type legacyAuthority struct {
	Partition string `json:"partition"`
	Epoch     int64  `json:"epoch"`
}

type legacyEvent struct {
	HazardType string         `json:"hazard_type"`
	Severity   string         `json:"severity"`
	AreaName   string         `json:"area_name"`
	Latitude   *float64       `json:"latitude"`
	Longitude  *float64       `json:"longitude"`
	OccurredAt string         `json:"occurred_at"`
	Attributes map[string]any `json:"attributes"`
}

type legacyMutation struct {
	Version       int64           `json:"version"`
	Operation     string          `json:"operation"`
	Authority     legacyAuthority `json:"authority"`
	CorrelationID string          `json:"correlation_id"`
	Event         *legacyEvent    `json:"event"`
}

type server struct {
	store *canonical.Store
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func main() {
	dsn := env("CANONICAL_DSN", "")
	if dsn == "" {
		log.Fatal("CANONICAL_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := canonical.Open(ctx, dsn, env("KAFKA_TOPIC_HAZARD_EVENTS", "hazard-events"))
	if err != nil {
		log.Fatal(err)
	}
	defer store.DB.Close()
	if os.Getenv("CANONICAL_REQUIRE_REPLICATION") == "false" {
		store.RequireReplication = false
	}
	s := &server{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /events", s.list)
	mux.HandleFunc("GET /v1/events", s.query)
	mux.HandleFunc("GET /events/hazard/{id}", s.getHazard)
	mux.HandleFunc("GET /events/{ref}", s.get)
	mux.HandleFunc("GET /internal/audit/events", s.auditEvents)
	mux.HandleFunc("GET /internal/audit/checksum", s.auditChecksum)
	mux.HandleFunc("GET /internal/audit/location", s.auditLocation)
	mux.HandleFunc("GET /analytics", s.analyticsPage)
	mux.HandleFunc("GET /internal/analytics/summary", s.analyticsSummary)
	mux.HandleFunc("PUT /internal/canonical/events/{source}/{ref}", s.put)
	mux.HandleFunc("PUT /internal/admin/events/{source}/{ref}", s.put)
	addr := env("ADDR", ":8080")
	log.Printf("canonical-api listening addr=%s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	var ok bool
	if err := s.store.DB.QueryRowContext(ctx, `SELECT to_regclass('canonical.events') IS NOT NULL`).Scan(&ok); err != nil || !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "canonical schema unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	events, err := s.store.List(r.Context(), q.Get("source"), "", q.Get("since"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *server) query(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	page, err := s.store.Query(r.Context(), canonical.Query{
		Source: q.Get("source"), Since: q.Get("since"), Limit: limit, Cursor: q.Get("cursor"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	event, err := s.store.GetBySourceRef(r.Context(), r.PathValue("ref"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, event)
}

func (s *server) getHazard(w http.ResponseWriter, r *http.Request) {
	event, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, event)
}

func (s *server) put(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	ref := r.PathValue("ref")
	var body legacyMutation
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "invalid_request", "message": "invalid JSON body"}})
		return
	}
	if body.Operation == "" {
		body.Operation = "upsert"
	}
	partition := body.Authority.Partition
	if partition == "" {
		partition = source
	}
	epoch := body.Authority.Epoch
	if epoch < 1 {
		epoch = 1
	}
	// The legacy protocol does not have a separate authority service. Install
	// the same fenced authority row before using the platform write path.
	_ = s.store.InstallLease(r.Context(), canonical.Lease{
		Authority: canonical.Authority{Partition: partition, Epoch: epoch},
		Owner:     "legacy-canonical-store",
		ExpiresAt: time.Now().Add(24 * time.Hour),
	})
	principal := canonical.Principal{Owner: "legacy-canonical-store", Assignments: map[string][]string{
		partition: {source},
	}}
	mutation := canonical.Mutation{
		Version:       body.Version,
		Operation:     body.Operation,
		Authority:     canonical.Authority{Partition: partition, Epoch: epoch},
		CorrelationID: body.CorrelationID,
	}
	if mutation.CorrelationID == "" {
		mutation.CorrelationID = "legacy-" + ref
	}
	if body.Event != nil {
		occurred, err := time.Parse(time.RFC3339Nano, body.Event.OccurredAt)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "invalid_request", "message": "invalid occurred_at"}})
			return
		}
		mutation.Event = &canonical.Input{
			HazardType: body.Event.HazardType, Severity: body.Event.Severity, AreaName: body.Event.AreaName,
			Latitude: body.Event.Latitude, Longitude: body.Event.Longitude, OccurredAt: occurred,
			Attributes: body.Event.Attributes,
		}
	}
	result, err := s.store.Put(r.Context(), principal, source, ref, mutation)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{
		"hazard_id": result.HazardID, "version": result.Version, "replayed": result.Replayed,
		"operation": result.Operation, "durability": result.Durability,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	var apiErr *canonical.Error
	if errors.As(err, &apiErr) {
		writeJSON(w, apiErr.Status, map[string]any{"error": map[string]string{"code": apiErr.Code, "message": apiErr.Message}})
		return
	}
	if strings.Contains(strings.ToLower(err.Error()), "not found") || strings.Contains(strings.ToLower(err.Error()), "no rows") {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": map[string]string{"code": "not_found", "message": "not found"}})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]string{"code": "storage_unavailable", "message": "canonical storage unavailable"}})
}
