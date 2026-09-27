package middleware

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

type contextKey string

const CorrelationIDKey contextKey = "correlation_id"

// CorrelationID generates or accepts a correlation ID and adds it to context.
func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corrID := r.Header.Get("X-Correlation-ID")
		if corrID == "" {
			corrID = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), CorrelationIDKey, corrID)
		w.Header().Set("X-Correlation-ID", corrID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetCorrelationID extracts the correlation ID from context.
func GetCorrelationID(ctx context.Context) string {
	if v, ok := ctx.Value(CorrelationIDKey).(string); ok {
		return v
	}
	return ""
}
