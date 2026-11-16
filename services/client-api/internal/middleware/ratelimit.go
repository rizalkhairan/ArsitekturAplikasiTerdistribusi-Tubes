package middleware

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/rizalkhairan/distributed-system/client-api/internal/auth"
	"github.com/rizalkhairan/distributed-system/observability"
)

// PriorityLimiter reserves capacity for Tim Lapangan (field) under overload.
type PriorityLimiter struct {
	total    int
	reserved int
	fieldSem chan struct{}
	mediaSem chan struct{}
	inFlight atomic.Int64
	rejected *obs.CounterVec
}

func NewPriorityLimiter(total, reserved int, registry *obs.Registry) *PriorityLimiter {
	if reserved < 0 {
		reserved = 0
	}
	if reserved >= total {
		reserved = total / 2
	}
	l := &PriorityLimiter{
		total: total, reserved: reserved,
		fieldSem: make(chan struct{}, total),
		mediaSem: make(chan struct{}, total-reserved),
	}
	if registry != nil {
		l.rejected = registry.Counter("admission_rejected_total", "Controlled rejections by role and reason", "role", "reason")
	}
	return l
}

func (l *PriorityLimiter) InFlight() int64 { return l.inFlight.Load() }

func (l *PriorityLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/hazards") {
			next.ServeHTTP(w, r)
			return
		}
		role := "media"
		if claims, err := auth.ValidateToken(r.Header.Get("Authorization")); err == nil && claims.Role == "field" {
			role = "field"
		}
		pool := l.mediaSem
		reason := "media_capacity"
		if role == "field" {
			pool = l.fieldSem
			reason = "field_capacity"
		}
		select {
		case pool <- struct{}{}:
			defer func() { <-pool }()
			l.inFlight.Add(1)
			defer l.inFlight.Add(-1)
			next.ServeHTTP(w, r)
		default:
			if l.rejected != nil {
				l.rejected.WithLabelValues(role, reason).Inc()
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", strconv.Itoa(1))
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]string{
					"code":    "shed",
					"message": "server is at capacity for this client class; retry later",
				},
			})
		}
	})
}

// RateLimiter is retained for callers that need a single-class limiter.
type RateLimiter struct {
	sem chan struct{}
}

func NewRateLimiter(maxConcurrent int) *RateLimiter {
	return &RateLimiter{sem: make(chan struct{}, maxConcurrent)}
}

func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case rl.sem <- struct{}{}:
			defer func() { <-rl.sem }()
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "too many requests, try again later"})
		}
	})
}
