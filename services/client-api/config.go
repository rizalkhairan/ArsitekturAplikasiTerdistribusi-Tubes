package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rizalkhairan/distributed-system/client-api/internal/auth"
)

type config struct {
	Port              string
	BNPBURL           string
	UpstreamTimeout   time.Duration
	MaxConcurrent      int
	BNPBInternalKey   string
	MaxLimit          int
	AuthServiceURL    string
	JWTSecret         string
}

func loadConfig() config {
	// Configure token TTL (default 60s, configurable for demo)
	if v := os.Getenv("TOKEN_TTL_SECONDS"); v != "" {
		if secs, err := time.ParseDuration(v + "s"); err == nil && secs > 0 {
			auth.SetMaxTokenAge(secs)
		}
	}

	return config{
		Port:            env("PORT", "8080"),
		BNPBURL:         env("BNPB_API_URL", "http://bnpb:8085"),
		UpstreamTimeout: envDuration("UPSTREAM_TIMEOUT", 5*time.Second),
		MaxConcurrent:   envInt("MAX_CONCURRENT_REQUESTS", 100),
		BNPBInternalKey: os.Getenv("BNPB_INTERNAL_KEY"),
		MaxLimit:        envInt("MAX_LIMIT", 100),
		AuthServiceURL:  env("AUTH_SERVICE_URL", "http://auth:8000"),
		JWTSecret:       os.Getenv("JWT_SECRET"),
	}
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return fallback
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

func initAuth(cfg config) error {
	if cfg.JWTSecret == "" {
		return fmt.Errorf("JWT_SECRET is required")
	}
	auth.InitAuth(cfg.AuthServiceURL, cfg.JWTSecret)
	return nil
}
