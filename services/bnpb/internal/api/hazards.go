package api

import (
	"compress/gzip"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
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
				writeHazardJSON(w, r, cached)
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
	writeHazardJSON(w, r, events)
}

// writeHazardJSON keeps the hot read path small on the wire. Hazard summaries
// contain repeated attribute keys and compress well; this matters when load is
// spread across a small demo cluster.
func writeHazardJSON(w http.ResponseWriter, r *http.Request, value any) {
	w.Header().Set("Content-Type", "application/json")
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")
		gz := gzip.NewWriter(w)
		_ = json.NewEncoder(gz).Encode(value)
		_ = gz.Close()
		return
	}
	_ = json.NewEncoder(w).Encode(value)
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
