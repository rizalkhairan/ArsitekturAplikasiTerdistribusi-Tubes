package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/health"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/middleware"
)

type Server struct {
	db                 *sql.DB
	server             *http.Server
	internalKey        string
	health             *health.Registry
	stalenessThreshold time.Duration
}

func NewServer(addr string, db *sql.DB, internalKey string, registry *health.Registry, stalenessThreshold time.Duration) *Server {
	s := &Server{db: db, internalKey: internalKey, health: registry, stalenessThreshold: stalenessThreshold}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /hazards", s.handleListHazards)
	mux.HandleFunc("GET /hazards/{id}", s.handleGetHazard)

	// Middleware chain: correlation → logging → internal auth → routes
	handler := middleware.CorrelationID(
		middleware.Logging(
			s.internalAuth(mux),
		),
	)

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
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		key := r.Header.Get("X-Internal-Key")
		if key == "" || key != s.internalKey {
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
