package middleware

import (
	"encoding/json"
	"net/http"
)

// RateLimiter limits concurrent requests using a semaphore.
type RateLimiter struct {
	sem chan struct{}
}

// NewRateLimiter creates a rate limiter with the given concurrency limit.
func NewRateLimiter(maxConcurrent int) *RateLimiter {
	return &RateLimiter{
		sem: make(chan struct{}, maxConcurrent),
	}
}

// Middleware wraps a handler with rate limiting.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case rl.sem <- struct{}{}:
			defer func() { <-rl.sem }()
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{"error": "too many requests, try again later"})
		}
	})
}


