package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/canonical"
)

func (s *Server) handleCanonicalEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	var err error
	if q.Has("limit") {
		limit, err = strconv.Atoi(q.Get("limit"))
		if limit < 1 || limit > 1000 {
			err = &canonical.Error{Status: 400, Code: "invalid_request", Message: "limit must be 1..1000"}
		}
	}
	var page canonical.Page
	if err == nil {
		page, err = s.canonical.Query(r.Context(), canonical.Query{Source: q.Get("source"), Since: q.Get("since"), Limit: limit, Cursor: q.Get("cursor")})
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		writeCanonicalError(w, err)
		return
	}
	json.NewEncoder(w).Encode(page)
}

func writeCanonicalError(w http.ResponseWriter, err error) {
	e := &canonical.Error{Status: 503, Code: "storage_unavailable", Message: "canonical query unavailable; retry this request"}
	errors.As(err, &e)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(e.Status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": e.Code, "message": e.Message}})
}
