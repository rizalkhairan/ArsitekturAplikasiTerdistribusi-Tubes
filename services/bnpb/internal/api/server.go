package api

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/dlqclient"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/health"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/middleware"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/processing"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/storeclient"
	"github.com/rizalkhairan/distributed-system/observability"
)

type listCacheEntry struct {
	events  []database.HazardEvent
	expires time.Time
}

type FreshnessFunc func(ctx context.Context, source string) (lastSuccess time.Time, stale bool, message string)

type Server struct {
	db                 *sql.DB
	writeDB            *sql.DB
	topic              string
	server             *http.Server
	internalKey        string
	health             *health.Registry
	stalenessThreshold time.Duration

	store          *storeclient.Client
	dlq            *dlqclient.Client
	auditToken     string
	freshness      FreshnessFunc
	sensorStatus   func(context.Context) any
	sensorFallback func() ([]database.HazardEvent, time.Time, bool)
	metrics        http.Handler
	outer          func(http.Handler) http.Handler
	ready          func() bool
	cacheMu        sync.Mutex
	listCache      map[string]listCacheEntry
	cacheTTL       time.Duration
}

func NewServer(addr string, db, writeDB *sql.DB, topic, internalKey string, registry *health.Registry, stalenessThreshold time.Duration) *Server {
	s := &Server{db: db, writeDB: writeDB, topic: topic, internalKey: internalKey, health: registry, stalenessThreshold: stalenessThreshold,
		listCache: map[string]listCacheEntry{}, cacheTTL: 500 * time.Millisecond}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /hazards", s.handleListHazards)
	mux.HandleFunc("GET /hazards/{id}", s.handleGetHazard)
	mux.HandleFunc("POST /internal/reprocess", s.handleReprocess)
	mux.HandleFunc("GET /sensor/status", s.handleSensorStatus)
	mux.HandleFunc("GET /internal/audit/events", s.auditAuth(s.handleAuditEvents))
	mux.HandleFunc("GET /internal/audit/checksum", s.auditAuth(s.handleAuditChecksum))
	mux.HandleFunc("GET /internal/audit/location", s.auditAuth(s.handleAuditLocation))
	mux.HandleFunc("GET /internal/audit/unprocessed", s.auditAuth(s.handleAuditUnprocessed))

	handler := middleware.CorrelationID(
		middleware.Logging(
			internalAuth(mux, internalKey),
		),
	)

	s.server = &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

type reprocessRequest struct {
	MessageID        string `json:"message_id"`
	Source           string `json:"source"`
	SourceRefID      string `json:"source_ref_id"`
	ProcessorVersion string `json:"processor_version"`
	RawBase64        string `json:"raw_base64"`
}

// handleReprocess is the authenticated canonical-write adapter used by C's DLQ
// replay tool. In M2 mode it writes through the canonical store; otherwise it
// preserves the M1 SQLite behavior.
func (s *Server) handleReprocess(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "reprocess payload too large")
		return
	}
	var req reprocessRequest
	if err := json.Unmarshal(body, &req); err != nil || req.Source != "PVMBG" || req.MessageID == "" || req.RawBase64 == "" {
		writeError(w, http.StatusBadRequest, "invalid PVMBG reprocess request")
		return
	}
	raw, err := base64.StdEncoding.DecodeString(req.RawBase64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "raw_base64 must be valid base64")
		return
	}
	version := req.ProcessorVersion
	if version == "" {
		version = "pvmbg-v2"
	}
	event, err := processing.MapVolcanicReport(r.Context(), s.db, raw, version)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if req.SourceRefID != "" && req.SourceRefID != event.SourceRefID && !strings.HasPrefix(event.SourceRefID, "pvmbg-v2-") {
		writeError(w, http.StatusConflict, "replayed source_ref_id does not match the mapped record")
		return
	}
	if s.store != nil {
		if err := s.store.UpsertAdmin(r.Context(), event, 1); err != nil {
			writeError(w, http.StatusInternalServerError, "canonical reprocess write failed")
			return
		}
	} else {
		if _, err := database.SaveHazards(r.Context(), s.writeDB, []database.HazardEvent{event}, database.Cursors{}, s.topic); err != nil {
			writeError(w, http.StatusInternalServerError, "canonical reprocess write failed")
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"message_id": req.MessageID, "source": "PVMBG", "source_ref_id": event.SourceRefID, "status": "processed", "idempotent": true})
}

// ConfigureM2 attaches the shared storage/DLQ path and the audit proxy.
func (s *Server) ConfigureM2(store *storeclient.Client, dlq *dlqclient.Client, auditToken string,
	freshness FreshnessFunc, sensorStatus func(context.Context) any) {
	s.store = store
	s.dlq = dlq
	s.auditToken = auditToken
	s.freshness = freshness
	s.sensorStatus = sensorStatus
}

func (s *Server) SetMetrics(handler http.Handler) { s.metrics = handler }

func (s *Server) ConfigureObservability(mw *obs.HTTPMiddleware) {
	if mw != nil {
		s.outer = mw.Wrap
	}
}

func (s *Server) SetReady(fn func() bool) { s.ready = fn }

func (s *Server) SetReadCacheTTL(ttl time.Duration) {
	if ttl >= 0 {
		s.cacheTTL = ttl
	}
}

// SetSensorFallback installs the last-known-good sensor snapshot used when the
// shared storage is temporarily unreachable.
func (s *Server) SetSensorFallback(fn func() ([]database.HazardEvent, time.Time, bool)) {
	s.sensorFallback = fn
}

// listEvents is a short read-through cache. The storage contract is a shared
// service; serving every 10k-VU read from it would make it the scaling
// bottleneck. A 500ms cache bounds staleness far below the 10s M1 criterion.
func (s *Server) listEvents(ctx context.Context, source, severity, since string, limit int) ([]database.HazardEvent, error) {
	key := source + "\x00" + severity + "\x00" + since + "\x00" + strconv.Itoa(limit)
	now := time.Now()
	s.cacheMu.Lock()
	if entry, ok := s.listCache[key]; ok && now.Before(entry.expires) {
		events := entry.events
		s.cacheMu.Unlock()
		return events, nil
	}
	s.cacheMu.Unlock()

	events, err := s.store.List(ctx, source, since, limit)
	if err != nil {
		return nil, err
	}
	if severity != "" {
		filtered := events[:0]
		for _, event := range events {
			if event.Severity == severity {
				filtered = append(filtered, event)
			}
		}
		events = filtered
	}
	if s.cacheTTL > 0 {
		s.cacheMu.Lock()
		s.listCache[key] = listCacheEntry{events: events, expires: now.Add(s.cacheTTL)}
		if len(s.listCache) > 512 {
			s.listCache = map[string]listCacheEntry{key: {events: events, expires: now.Add(s.cacheTTL)}}
		}
		s.cacheMu.Unlock()
	}
	return events, nil
}

// internalAuth checks the shared secret for internal service-to-service calls.
func internalAuth(next http.Handler, internalKey string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/health", r.URL.Path == "/readyz", r.URL.Path == "/metrics", r.URL.Path == "/sensor/status":
			next.ServeHTTP(w, r)
			return
		case strings.HasPrefix(r.URL.Path, "/internal/audit/"):
			next.ServeHTTP(w, r) // auditAuth enforces its own Bearer token
			return
		}
		key := r.Header.Get("X-Internal-Key")
		if key == "" || key != internalKey {
			writeError(w, http.StatusUnauthorized, "unauthorized: invalid internal key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) auditAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.auditToken == "" {
			writeContractError(w, http.StatusServiceUnavailable, "audit_unconfigured", "audit proxy is not configured")
			return
		}
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(header, "Bearer ")) != s.auditToken {
			writeContractError(w, http.StatusUnauthorized, "unauthorized", "invalid audit token")
			return
		}
		next(w, r)
	}
}

func (s *Server) Start() error {
	handler := s.server.Handler
	if s.outer != nil {
		handler = s.outer(handler)
	}
	s.server.Handler = handler
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleReady is the readiness used by Kubernetes. A pod that cannot reach the
// shared storage cannot serve correct reads, so it is removed from the Service.
// Losing the coordinator stops polling but does not break reads.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.ready != nil && !s.ready() {
		writeContractError(w, http.StatusServiceUnavailable, "draining", "instance is draining")
		return
	}
	if s.store != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.store.Ready(ctx); err != nil {
			writeContractError(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.metrics == nil {
		writeContractError(w, http.StatusNotFound, "not_found", "metrics are not configured")
		return
	}
	s.metrics.ServeHTTP(w, r)
}

// writeFreshnessHeaders attaches an explicit data-freshness marker to every hazard
// response. It prefers shared (coordinator/storage) freshness in M2 mode so a
// non-leader replica does not report its own empty poller state.
func (s *Server) writeFreshnessHeaders(w http.ResponseWriter, r *http.Request, sources ...string) {
	if len(sources) == 0 {
		sources = []string{"BMKG", "PVMBG", "SENSOR_PANTAI"}
	}
	degraded := false
	for _, source := range sources {
		status := "ok"
		var lastSuccess time.Time
		var message string
		if s.freshness != nil {
			success, degradedSource, msg := s.freshness(r.Context(), source)
			lastSuccess, message = success, msg
			if degradedSource || lastSuccess.IsZero() {
				status = "stale"
			}
		} else if s.health != nil {
			stale, success, _, msg := s.health.SourceStatus(source, s.stalenessThreshold)
			lastSuccess, message = success, msg
			if stale {
				status = "stale"
			}
		}
		if status == "stale" {
			degraded = true
		}
		w.Header().Set("X-Source-"+source+"-Status", status)
		if !lastSuccess.IsZero() {
			w.Header().Set("X-Source-"+source+"-Last-Success", lastSuccess.Format(time.RFC3339))
		}
		if status == "stale" {
			if lastSuccess.IsZero() {
				w.Header().Set("X-Source-"+source+"-Stale-Since", time.Now().UTC().Format(time.RFC3339))
			} else {
				w.Header().Set("X-Source-"+source+"-Stale-Since", lastSuccess.Format(time.RFC3339))
			}
			if message != "" {
				w.Header().Set("X-Source-"+source+"-Error", message)
			}
		}
	}
	freshness := "ok"
	if degraded {
		freshness = "degraded"
	}
	w.Header().Set("X-Data-Freshness", freshness)
}

func (s *Server) handleSensorStatus(w http.ResponseWriter, r *http.Request) {
	if s.sensorStatus == nil {
		writeContractError(w, http.StatusNotFound, "not_found", "sensor path is not enabled")
		return
	}
	writeJSON(w, http.StatusOK, s.sensorStatus(r.Context()))
}

func (s *Server) handleAuditEvents(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeContractError(w, http.StatusServiceUnavailable, "storage_unavailable", "storage is not configured")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	response, err := s.store.AuditEvents(ctx, r.URL.Query().Get("source"), r.URL.Query().Get("since"),
		limit, r.URL.Query().Get("cursor"))
	if err != nil {
		writeContractError(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleAuditChecksum(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeContractError(w, http.StatusServiceUnavailable, "storage_unavailable", "storage is not configured")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	checksum, err := s.store.Checksum(ctx, r.URL.Query().Get("source"), r.URL.Query().Get("partial") == "true")
	if err != nil {
		writeContractError(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, checksum)
}

func (s *Server) handleAuditLocation(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeContractError(w, http.StatusServiceUnavailable, "storage_unavailable", "storage is not configured")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	location, err := s.store.Location(ctx, r.URL.Query().Get("hazard_id"))
	if err != nil {
		writeContractError(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, location)
}

func (s *Server) handleAuditUnprocessed(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		writeContractError(w, http.StatusServiceUnavailable, "dlq_unavailable", "DLQ is not configured")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	response, err := s.dlq.Unprocessed(ctx, r.URL.Query().Get("source"), r.URL.Query().Get("since"),
		limit, r.URL.Query().Get("partial") == "true")
	if err != nil {
		writeContractError(w, http.StatusServiceUnavailable, "dlq_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeContractError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
