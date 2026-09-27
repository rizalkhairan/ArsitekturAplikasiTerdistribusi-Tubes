package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/middleware"
)

type Server struct {
	db          *sql.DB
	server      *http.Server
	internalKey string
}

func NewServer(addr string, db *sql.DB, internalKey string) *Server {
	s := &Server{db: db, internalKey: internalKey}
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

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
