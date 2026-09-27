package main

import (
	"fmt"
	"os"
	"time"
)

type config struct {
	DBPath          string
	BMKGAddress     string
	BMKGKey         string
	PVMBGURL        string
	PVMBGToken      string
	UpstreamTimeout time.Duration
	PollInterval    time.Duration
	APIPort         string
	InternalAPIKey  string
}

func loadConfig() (config, error) {
	cfg := config{
		DBPath:         env("DB_PATH", "./data/bnpb.db"),
		BMKGAddress:    env("BMKG_GRPC_ADDR", "localhost:8081"),
		BMKGKey:        os.Getenv("BMKG_API_KEY"),
		PVMBGURL:       env("PVMBG_BASE_URL", "http://localhost:8082"),
		PVMBGToken:     os.Getenv("PVMBG_TOKEN"),
		APIPort:        env("BNPB_API_PORT", "8085"),
		InternalAPIKey: os.Getenv("INTERNAL_API_KEY"),
	}
	var err error
	cfg.UpstreamTimeout, err = time.ParseDuration(env("UPSTREAM_TIMEOUT", "5s"))
	if err != nil || cfg.UpstreamTimeout <= 0 {
		return cfg, fmt.Errorf("UPSTREAM_TIMEOUT must be a positive duration, such as 5s")
	}
	cfg.PollInterval, err = time.ParseDuration(env("POLL_INTERVAL", "3s"))
	if err != nil || cfg.PollInterval <= 0 {
		return cfg, fmt.Errorf("POLL_INTERVAL must be a positive duration, such as 3s")
	}
	if cfg.BMKGKey == "" || cfg.PVMBGToken == "" {
		return cfg, fmt.Errorf("BMKG_API_KEY and PVMBG_TOKEN are required")
	}
	if cfg.InternalAPIKey == "" {
		return cfg, fmt.Errorf("INTERNAL_API_KEY is required for read API authentication")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
