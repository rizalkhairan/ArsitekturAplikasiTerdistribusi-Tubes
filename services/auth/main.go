package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rizalkhairan/distributed-system/auth/internal/database"
	"github.com/rizalkhairan/distributed-system/auth/internal/handlers"
	"github.com/rizalkhairan/distributed-system/shared/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	t, err := telemetry.StartService(ctx, "auth")
	if err != nil {
		log.Fatal(err)
	}
	defer t.Flush()

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

	server := &http.Server{Addr: ":" + port, Handler: t.Mux(mux), ReadHeaderTimeout: 5 * time.Second}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	t.Logger.Info("auth_started")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		t.Logger.Error("http_server_failed", "error", err)
		stop()
	}
	<-shutdownDone
}
