package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

func (s *Server) handleListHazards(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	events, err := database.ListHazards(r.Context(), s.db, q.Get("source"), q.Get("severity"), q.Get("since"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

func (s *Server) handleGetHazard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	event, err := database.GetHazard(r.Context(), s.db, id)
	if err != nil {
		writeError(w, http.StatusNotFound, "hazard not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(event)
}
