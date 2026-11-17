package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/canonical"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/health"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/middleware"
	"github.com/rizalkhairan/distributed-system/shared/telemetry"
)

type Server struct {
	canonical          *canonical.Store
	db                 *sql.DB
	server             *http.Server
	internalKey        string
	health             *health.Registry
	stalenessThreshold time.Duration
}

func NewServer(addr string, db *sql.DB, internalKey string, registry *health.Registry, stalenessThreshold time.Duration, runtimes ...*telemetry.Runtime) *Server {
	return newServer(addr, db, nil, nil, internalKey, registry, stalenessThreshold, runtimes...)
}

func NewCanonicalServer(addr string, store *canonical.Store, credentials map[string]canonical.Principal, internalKey string, registry *health.Registry, stalenessThreshold time.Duration, runtime *telemetry.Runtime) *Server {
	return newServer(addr, nil, store, credentials, internalKey, registry, stalenessThreshold, runtime)
}

func newServer(addr string, db *sql.DB, store *canonical.Store, credentials map[string]canonical.Principal, internalKey string, registry *health.Registry, stalenessThreshold time.Duration, runtimes ...*telemetry.Runtime) *Server {
	s := &Server{db: db, internalKey: internalKey, health: registry, stalenessThreshold: stalenessThreshold}
	s.canonical = store
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	if store != nil {
		mux.HandleFunc("GET /ready", s.handleReady)
	}
	mux.HandleFunc("GET /hazards", s.handleListHazards)
	mux.HandleFunc("GET /hazards/{id}", s.handleGetHazard)
	if store != nil {
		mux.HandleFunc("GET /internal/canonical/events", s.handleCanonicalEvents)
		mux.Handle("/internal/canonical/", canonical.Handler(store, credentials))
	}

	var handler http.Handler
	if len(runtimes) > 0 && runtimes[0] != nil {
		handler = runtimes[0].Mux(mux, s.internalAuth)
	} else {
		handler = middleware.CorrelationID(middleware.Logging(s.internalAuth(mux)))
	}

	s.server = &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// internalAuth checks the shared secret for internal service-to-service calls.
func (s *Server) internalAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readRoute := r.URL.Path == "/internal/canonical/events"
		if r.URL.Path == "/health" || (s.canonical != nil && (r.URL.Path == "/ready" || (strings.HasPrefix(r.URL.Path, "/internal/canonical/") && !readRoute))) {
			next.ServeHTTP(w, r)
			return
		}
		key := r.Header.Get("X-Internal-Key")
		if key == "" || key != s.internalKey {
			if s.canonical != nil && readRoute {
				writeCanonicalError(w, &canonical.Error{Status: 401, Code: "unauthorized", Message: "valid internal read key required"})
				return
			}
			writeError(w, http.StatusUnauthorized, "unauthorized: invalid internal key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) Start() error {
	log.Printf("BNPB read API listening on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	var ready bool
	err := s.canonical.DB.QueryRowContext(ctx, `SELECT to_regclass('canonical.events') IS NOT NULL`).Scan(&ready)
	if err != nil || !ready {
		writeError(w, 503, "canonical database/schema unavailable")
		return
	}
	s.handleHealth(w, r)
}

// writeFreshnessHeaders attaches an explicit data-freshness marker to every hazard
// response. When a source is stale, the response still returns the last known Canonical Store data
func (s *Server) writeFreshnessHeaders(w http.ResponseWriter, sources ...string) {
	if s.health == nil {
		return
	}
	if len(sources) == 0 {
		sources = []string{"BMKG", "PVMBG"}
	}
	degraded := false
	for _, source := range sources {
		stale, lastSuccess, _, message := s.health.SourceStatus(source, s.stalenessThreshold)
		status := "ok"
		if stale {
			status = "stale"
			degraded = true
		}
		w.Header().Set("X-Source-"+source+"-Status", status)
		if !lastSuccess.IsZero() {
			w.Header().Set("X-Source-"+source+"-Last-Success", lastSuccess.Format(time.RFC3339))
		}
		if stale {
			since := lastSuccess
			if since.IsZero() {
				since = time.Now().UTC()
			}
			w.Header().Set("X-Source-"+source+"-Stale-Since", since.Format(time.RFC3339))
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

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
