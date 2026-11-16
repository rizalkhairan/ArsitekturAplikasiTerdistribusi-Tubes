package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

func (s *Server) handleListHazards(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	var events []database.HazardEvent
	var err error
	if s.store != nil {
		events, err = s.listEvents(r.Context(), q.Get("source"), q.Get("severity"), q.Get("since"), limit)
		if err != nil && q.Get("source") == "SENSOR_PANTAI" && s.sensorFallback != nil {
			if cached, asOf, ok := s.sensorFallback(); ok {
				if severity := q.Get("severity"); severity != "" {
					filtered := cached[:0]
					for _, event := range cached {
						if event.Severity == severity {
							filtered = append(filtered, event)
						}
					}
					cached = filtered
				}
				if limit > 0 && len(cached) > limit {
					cached = cached[:limit]
				}
				w.Header().Set("X-Data-Freshness", "degraded")
				w.Header().Set("X-Source-SENSOR_PANTAI-Status", "stale")
				w.Header().Set("X-Source-SENSOR_PANTAI-Stale-Since", asOf.UTC().Format(time.RFC3339))
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(cached)
				return
			}
		}
	} else {
		events, err = database.ListHazards(r.Context(), s.db, q.Get("source"), q.Get("severity"), q.Get("since"), limit)
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if source := q.Get("source"); source != "" {
		s.writeFreshnessHeaders(w, r, source)
	} else {
		s.writeFreshnessHeaders(w, r)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(events)
}

func (s *Server) handleGetHazard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var event database.HazardEvent
	var err error
	if s.store != nil {
		// The client API uses hazard_id; the canonical contract uses source_ref_id.
		event, err = s.store.GetByHazardID(r.Context(), id)
	} else {
		event, err = database.GetHazard(r.Context(), s.db, id)
	}
	if err != nil {
		writeError(w, http.StatusNotFound, "hazard not found")
		return
	}
	s.writeFreshnessHeaders(w, r, event.Source)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(event)
}
