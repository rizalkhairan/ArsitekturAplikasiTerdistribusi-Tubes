package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rizalkhairan/distributed-system/auth/internal/database"
	"github.com/rizalkhairan/distributed-system/auth/internal/handlers"
	"github.com/rizalkhairan/distributed-system/auth/internal/middleware"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./data/auth.db"
	}

	db, err := database.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Seed users (passwords from env vars)
	if err := database.SeedUsers(db); err != nil {
		log.Fatal(err)
	}

	// Load JWT secret (required — fail fast if not set)
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatal("JWT_SECRET is required")
	}

	// Load token TTL
	accessTTL := 60 * time.Second
	if v := os.Getenv("ACCESS_TOKEN_TTL_SECONDS"); v != "" {
		if secs, err := time.ParseDuration(v + "s"); err == nil && secs > 0 {
			accessTTL = secs
		}
	}

	refreshTTL := 24 * time.Hour
	if v := os.Getenv("REFRESH_TOKEN_TTL_HOURS"); v != "" {
		if hrs, err := time.ParseDuration(v + "h"); err == nil && hrs > 0 {
			refreshTTL = hrs
		}
	}

	handler := handlers.NewHandler(db, jwtSecret, accessTTL, refreshTTL)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.HandleHealth)
	mux.HandleFunc("POST /auth/login", handler.HandleLogin)
	mux.HandleFunc("POST /auth/refresh", handler.HandleRefresh)

	// Middleware chain: correlation → logging → routes
	wrapped := middleware.CorrelationID(middleware.Logging(mux))

	log.Printf("Auth service listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, wrapped))
}
