package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rizalkhairan/distributed-system/client-api/internal/auth"
	"github.com/rizalkhairan/distributed-system/client-api/internal/bnpb"
	"github.com/rizalkhairan/distributed-system/client-api/internal/filter"
	"github.com/rizalkhairan/distributed-system/client-api/internal/middleware"
)

func main() {
	cfg := loadConfig()

	if err := initAuth(cfg); err != nil {
		log.Fatalf("Auth initialization failed: %v", err)
	}

	bnpbClient := bnpb.NewClient(cfg.BNPBURL, cfg.UpstreamTimeout, cfg.BNPBInternalKey)
	authProxy := auth.NewAuthProxy(cfg.AuthServiceURL)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /auth/login", handleLogin(authProxy))
	mux.HandleFunc("POST /auth/refresh", handleRefresh(authProxy))
	mux.HandleFunc("GET /hazards", authMiddleware(handleListHazards(bnpbClient, cfg.MaxLimit)))
	mux.HandleFunc("GET /hazards/{id}", authMiddleware(handleGetHazard(bnpbClient)))

	// Middleware chain: correlation → logging → rate limit → routes
	rateLimiter := middleware.NewRateLimiter(cfg.MaxConcurrent)
	handler := middleware.CorrelationID(
		middleware.Logging(
			rateLimiter.Middleware(mux),
		),
	)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("Client-Facing API listening on :%s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(shutdownCtx)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func handleLogin(proxy *auth.AuthProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.Username == "" || req.Password == "" {
			writeError(w, http.StatusBadRequest, "username and password are required")
			return
		}

		result, err := proxy.Login(middleware.GetCorrelationID(r.Context()), req.Username, req.Password)
		if err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

func handleRefresh(proxy *auth.AuthProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.RefreshToken == "" {
			writeError(w, http.StatusBadRequest, "refresh_token is required")
			return
		}

		result, err := proxy.Refresh(middleware.GetCorrelationID(r.Context()), req.RefreshToken)
		if err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, err := auth.ValidateToken(r.Header.Get("Authorization"))
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		// Reject media-scope clients explicitly requesting raw fields
		if auth.RejectRawFieldRequest(r, claims.Scope) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{"error": "forbidden: raw fields not accessible with summary scope"})
			return
		}

		// Store claims in context for downstream handlers
		ctx := context.WithValue(r.Context(), "claims", claims)
		next(w, r.WithContext(ctx))
	}
}

func handleListHazards(client *bnpb.Client, maxLimit int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := r.Context().Value("claims").(*auth.Claims)

		q := r.URL.Query()
		limit, err := strconv.Atoi(q.Get("limit"))
		if err != nil || limit < 1 || limit > maxLimit {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("limit must be between 1 and %d", maxLimit)})
			return
		}

		hazards, headers, err := client.ListHazards(r.Context(), q.Get("source"), q.Get("severity"), q.Get("since"), limit)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		bnpb.CopyFreshnessHeaders(w.Header(), headers)

		// Convert to maps and filter by scope
		maps := make([]map[string]any, len(hazards))
		for i, h := range hazards {
			maps[i] = filter.ToMap(h)
		}
		filtered := filter.FilterHazards(maps, claims.Scope)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(filtered)
	}
}

func handleGetHazard(client *bnpb.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := r.Context().Value("claims").(*auth.Claims)
		id := r.PathValue("id")

		hazard, headers, err := client.GetHazard(r.Context(), id)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			if err.Error() == "hazard not found" {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.WriteHeader(http.StatusBadGateway)
			}
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		bnpb.CopyFreshnessHeaders(w.Header(), headers)

		m := filter.ToMap(*hazard)
		filtered := filter.FilterHazard(m, claims.Scope)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(filtered)
	}
}
