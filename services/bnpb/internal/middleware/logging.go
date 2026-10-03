package middleware

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Logging logs structured JSON with correlation ID, method, path, status, and latency.
func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corrID := GetCorrelationID(r.Context())

		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapped, r)

		latency := time.Since(start)
		logJSON(map[string]any{
			"ts":          start.Format(time.RFC3339Nano),
			"corr_id":     corrID,
			"method":      r.Method,
			"path":        r.URL.Path,
			"status":      wrapped.statusCode,
			"latency_ms":  latency.Milliseconds(),
			"remote_addr": r.RemoteAddr,
		})
	})
}

func logJSON(data map[string]any) {
	b, err := json.Marshal(data)
	if err != nil {
		log.Printf(`{"error":"log marshal failed"}`)
		return
	}
	log.Print(string(b))
}
