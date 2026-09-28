package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	DBPath                   string
	BMKGAddress              string
	BMKGKey                  string
	PVMBGURL                 string
	PVMBGToken               string
	KafkaBrokers             []string
	KafkaTopic               string
	KafkaClientID            string
	KafkaPublishRetryLimit   int
	KafkaPublishRetryBackoff time.Duration
	OutboxPollInterval       time.Duration
	HealthAddress            string
	UpstreamTimeout          time.Duration
	PollInterval             time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		DBPath:        env("DB_PATH", "./data/bnpb.db"),
		BMKGAddress:   env("BMKG_GRPC_ADDR", "localhost:8081"),
		BMKGKey:       os.Getenv("BMKG_API_KEY"),
		PVMBGURL:      env("PVMBG_BASE_URL", "http://localhost:8082"),
		PVMBGToken:    os.Getenv("PVMBG_TOKEN"),
		KafkaBrokers:  strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ","),
		KafkaTopic:    env("KAFKA_TOPIC_HAZARD_EVENTS", "hazard-events"),
		KafkaClientID: env("KAFKA_CLIENT_ID", "bnpb-aggregator"),
		HealthAddress: env("HEALTH_ADDR", ":8080"),
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
	cfg.OutboxPollInterval, err = time.ParseDuration(env("OUTBOX_POLL_INTERVAL", "1s"))
	if err != nil || cfg.OutboxPollInterval <= 0 {
		return cfg, fmt.Errorf("OUTBOX_POLL_INTERVAL must be a positive duration, such as 1s")
	}
	cfg.KafkaPublishRetryLimit, err = strconv.Atoi(env("KAFKA_PUBLISH_RETRY_LIMIT", "3"))
	if err != nil || cfg.KafkaPublishRetryLimit < 1 {
		return cfg, fmt.Errorf("KAFKA_PUBLISH_RETRY_LIMIT must be a positive integer")
	}
	cfg.KafkaPublishRetryBackoff, err = time.ParseDuration(env("KAFKA_PUBLISH_RETRY_BACKOFF", "500ms"))
	if err != nil || cfg.KafkaPublishRetryBackoff <= 0 {
		return cfg, fmt.Errorf("KAFKA_PUBLISH_RETRY_BACKOFF must be a positive duration, such as 500ms")
	}
	if cfg.BMKGKey == "" || cfg.PVMBGToken == "" {
		return cfg, fmt.Errorf("BMKG_API_KEY and PVMBG_TOKEN are required")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
