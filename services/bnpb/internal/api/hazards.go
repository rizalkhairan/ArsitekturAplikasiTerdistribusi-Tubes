package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

func (s *Server) handleListHazards(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if s.canonical != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		events, err := s.canonical.List(ctx, q.Get("source"), q.Get("severity"), q.Get("since"), limit)
		if err != nil {
			writeError(w, 503, "canonical storage unavailable")
			return
		}
		s.writeFreshnessHeaders(w)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(events)
		return
	}
	events, err := database.ListHazards(r.Context(), s.db, q.Get("source"), q.Get("severity"), q.Get("since"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if source := q.Get("source"); source != "" {
		s.writeFreshnessHeaders(w, source)
	} else {
		s.writeFreshnessHeaders(w)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

func (s *Server) handleGetHazard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.canonical != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		event, err := s.canonical.Get(ctx, id)
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, 404, "hazard not found")
			return
		}
		if err != nil {
			writeError(w, 503, "canonical storage unavailable")
			return
		}
		s.writeFreshnessHeaders(w, event.Source)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(event)
		return
	}
	event, err := database.GetHazard(r.Context(), s.db, id)
	if err != nil {
		writeError(w, http.StatusNotFound, "hazard not found")
		return
	}
	s.writeFreshnessHeaders(w, event.Source)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(event)
}
