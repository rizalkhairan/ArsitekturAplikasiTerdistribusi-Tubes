package api

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/health"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/middleware"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/processing"
)

type Server struct {
	db                 *sql.DB
	writeDB            *sql.DB
	topic              string
	server             *http.Server
	internalKey        string
	health             *health.Registry
	stalenessThreshold time.Duration
}

func NewServer(addr string, db, writeDB *sql.DB, topic, internalKey string, registry *health.Registry, stalenessThreshold time.Duration) *Server {
	s := &Server{db: db, writeDB: writeDB, topic: topic, internalKey: internalKey, health: registry, stalenessThreshold: stalenessThreshold}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /hazards", s.handleListHazards)
	mux.HandleFunc("GET /hazards/{id}", s.handleGetHazard)
	mux.HandleFunc("POST /internal/reprocess", s.handleReprocess)

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

type reprocessRequest struct {
	MessageID        string `json:"message_id"`
	Source           string `json:"source"`
	SourceRefID      string `json:"source_ref_id"`
	ProcessorVersion string `json:"processor_version"`
	RawBase64        string `json:"raw_base64"`
}

// handleReprocess is the authenticated canonical-write adapter used by the
// C-owned DLQ replay tool. It is intentionally separate from public hazard APIs.
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
	if _, err := database.SaveHazards(r.Context(), s.writeDB, []database.HazardEvent{event}, database.Cursors{}, s.topic); err != nil {
		writeError(w, http.StatusInternalServerError, "canonical reprocess write failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"message_id": req.MessageID, "source": "PVMBG", "source_ref_id": event.SourceRefID, "status": "processed", "idempotent": true})
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
