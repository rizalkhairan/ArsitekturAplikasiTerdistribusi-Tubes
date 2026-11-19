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
	DBReadConnections        int
	BMKGAddress              string
	BMKGKey                  string
	PVMBGURL                 string
	PVMBGToken               string
	KafkaBrokers             []string
	KafkaEnabled             bool
	KafkaTopic               string
	KafkaClientID            string
	KafkaPublishRetryLimit   int
	KafkaPublishRetryBackoff time.Duration
	OutboxPollInterval       time.Duration
	HealthAddress            string
	APIPort                  string
	InternalAPIKey           string
	PipelineURL              string
	PipelineInternalToken    string
	UpstreamTimeout          time.Duration
	AuditTimeout             time.Duration
	PollInterval             time.Duration
	PVMBGBatchLimit          int
	BMKGBatchLimit           int
	StalenessThreshold       time.Duration
	ReadCacheTTL             time.Duration

	CoordinatorURL   string
	CoordinatorToken string
	StorageURL       string
	StorageToken     string
	DLQURL           string
	DLQToken         string
	AuditToken       string
	NotifierURL      string
	NotifierToken    string
	InstanceID       string
	LeaseTTL         time.Duration
	LeaseRenew       time.Duration
	LeaseSafety      time.Duration
	MirrorLocal      bool

	SensorURL             string
	SensorToken           string
	SensorPollInterval    time.Duration
	SensorTimeout         time.Duration
	SensorStaleAfter      time.Duration
	SensorPageLimit       int
	SensorMaxPages        int
	SensorOverlap         time.Duration
	SensorStationsRefresh time.Duration
	SensorQualityEnum     []string
	SensorSeaMin          float64
	SensorSeaMax          float64
	SensorWaveMin         float64
	SensorWaveMax         float64
	SensorMaxFuture       time.Duration
	SensorSiagaWave       float64
	SensorSiagaSea        float64
	SensorWaspadaWave     float64
	SensorWaspadaSea      float64
}

func loadConfig() (config, error) {
	cfg := config{
		DBPath:                env("DB_PATH", "./data/bnpb.db"),
		BMKGAddress:           env("BMKG_GRPC_ADDR", "localhost:8081"),
		BMKGKey:               os.Getenv("BMKG_API_KEY"),
		PVMBGURL:              env("PVMBG_BASE_URL", "http://localhost:8082"),
		PVMBGToken:            os.Getenv("PVMBG_TOKEN"),
		KafkaBrokers:          strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ","),
		KafkaEnabled:          envBool("KAFKA_ENABLED", true),
		KafkaTopic:            env("KAFKA_TOPIC_HAZARD_EVENTS", "hazard-events"),
		KafkaClientID:         env("KAFKA_CLIENT_ID", "bnpb-aggregator"),
		HealthAddress:         env("HEALTH_ADDR", ":8080"),
		APIPort:               env("BNPB_API_PORT", "8085"),
		InternalAPIKey:        os.Getenv("INTERNAL_API_KEY"),
		PipelineURL:           env("INGEST_PIPELINE_URL", "http://ingest-pipeline:8094"),
		PipelineInternalToken: os.Getenv("PIPELINE_INTERNAL_TOKEN"),
		CoordinatorURL:        os.Getenv("COORDINATOR_URL"),
		CoordinatorToken:      os.Getenv("COORDINATOR_TOKEN"),
		StorageURL:            os.Getenv("STORAGE_URL"),
		StorageToken:          os.Getenv("STORAGE_TOKEN"),
		AuditToken:            os.Getenv("AUDIT_TOKEN"),
		NotifierURL:           os.Getenv("NOTIFIER_URL"),
		NotifierToken:         os.Getenv("NOTIFIER_TOKEN"),
		InstanceID:            env("INSTANCE_ID", hostnameOr("bnpb-local")),
		MirrorLocal:           envBool("MIRROR_LOCAL_DB", false),
		SensorURL:             os.Getenv("SENSOR_URL"),
		SensorToken:           os.Getenv("PANTAI_AUTH_TOKEN"),
	}
	cfg.DLQURL = env("DLQ_URL", cfg.PipelineURL)
	cfg.DLQToken = env("DLQ_TOKEN", cfg.PipelineInternalToken)

	var err error
	cfg.DBReadConnections, err = strconv.Atoi(env("DB_READ_CONNECTIONS", "8"))
	if err != nil || cfg.DBReadConnections < 1 {
		return cfg, fmt.Errorf("DB_READ_CONNECTIONS must be a positive integer")
	}
	cfg.UpstreamTimeout, err = time.ParseDuration(env("UPSTREAM_TIMEOUT", "5s"))
	if err != nil || cfg.UpstreamTimeout <= 0 {
		return cfg, fmt.Errorf("UPSTREAM_TIMEOUT must be a positive duration, such as 5s")
	}
	cfg.AuditTimeout, err = time.ParseDuration(env("AUDIT_TIMEOUT", "90s"))
	if err != nil || cfg.AuditTimeout <= 0 {
		return cfg, fmt.Errorf("AUDIT_TIMEOUT must be a positive duration, such as 90s")
	}
	cfg.PollInterval, err = time.ParseDuration(env("POLL_INTERVAL", "3s"))
	if err != nil || cfg.PollInterval <= 0 {
		return cfg, fmt.Errorf("POLL_INTERVAL must be a positive duration, such as 3s")
	}
	cfg.PVMBGBatchLimit, err = strconv.Atoi(env("PVMBG_BATCH_LIMIT", "100"))
	if err != nil || cfg.PVMBGBatchLimit < 1 {
		return cfg, fmt.Errorf("PVMBG_BATCH_LIMIT must be a positive integer")
	}
	cfg.BMKGBatchLimit, err = strconv.Atoi(env("BMKG_BATCH_LIMIT", "100"))
	if err != nil || cfg.BMKGBatchLimit < 1 {
		return cfg, fmt.Errorf("BMKG_BATCH_LIMIT must be a positive integer")
	}
	cfg.OutboxPollInterval, err = time.ParseDuration(env("OUTBOX_POLL_INTERVAL", "1s"))
	if err != nil || cfg.OutboxPollInterval <= 0 {
		return cfg, fmt.Errorf("OUTBOX_POLL_INTERVAL must be a positive duration, such as 1s")
	}
	cfg.StalenessThreshold, err = time.ParseDuration(env("STALENESS_THRESHOLD", "10s"))
	if err != nil || cfg.StalenessThreshold <= 0 {
		return cfg, fmt.Errorf("STALENESS_THRESHOLD must be a positive duration, such as 10s")
	}
	cfg.ReadCacheTTL, err = time.ParseDuration(env("READ_CACHE_TTL", "5s"))
	if err != nil || cfg.ReadCacheTTL < 0 {
		return cfg, fmt.Errorf("READ_CACHE_TTL must be a non-negative duration, such as 2s")
	}
	cfg.KafkaPublishRetryLimit, err = strconv.Atoi(env("KAFKA_PUBLISH_RETRY_LIMIT", "3"))
	if err != nil || cfg.KafkaPublishRetryLimit < 1 {
		return cfg, fmt.Errorf("KAFKA_PUBLISH_RETRY_LIMIT must be a positive integer")
	}
	cfg.KafkaPublishRetryBackoff, err = time.ParseDuration(env("KAFKA_PUBLISH_RETRY_BACKOFF", "500ms"))
	if err != nil || cfg.KafkaPublishRetryBackoff <= 0 {
		return cfg, fmt.Errorf("KAFKA_PUBLISH_RETRY_BACKOFF must be a positive duration, such as 500ms")
	}
	if cfg.LeaseTTL, err = time.ParseDuration(env("LEASE_TTL", "15s")); err != nil || cfg.LeaseTTL <= 0 {
		return cfg, fmt.Errorf("LEASE_TTL must be a positive duration")
	}
	if cfg.LeaseRenew, err = time.ParseDuration(env("LEASE_RENEW_INTERVAL", "5s")); err != nil || cfg.LeaseRenew <= 0 {
		return cfg, fmt.Errorf("LEASE_RENEW_INTERVAL must be a positive duration")
	}
	if cfg.LeaseSafety, err = time.ParseDuration(env("LEASE_SAFETY_MARGIN", "2s")); err != nil || cfg.LeaseSafety < 0 {
		return cfg, fmt.Errorf("LEASE_SAFETY_MARGIN must be a non-negative duration")
	}
	if cfg.SensorPollInterval, err = time.ParseDuration(env("SENSOR_POLL_INTERVAL", "2s")); err != nil || cfg.SensorPollInterval <= 0 {
		return cfg, fmt.Errorf("SENSOR_POLL_INTERVAL must be a positive duration")
	}
	if cfg.SensorTimeout, err = time.ParseDuration(env("SENSOR_TIMEOUT", "3s")); err != nil || cfg.SensorTimeout <= 0 {
		return cfg, fmt.Errorf("SENSOR_TIMEOUT must be a positive duration")
	}
	if cfg.SensorStaleAfter, err = time.ParseDuration(env("SENSOR_STALE_AFTER", "45s")); err != nil || cfg.SensorStaleAfter <= 0 {
		return cfg, fmt.Errorf("SENSOR_STALE_AFTER must be a positive duration")
	}
	if cfg.SensorOverlap, err = time.ParseDuration(env("SENSOR_CURSOR_OVERLAP", "5m")); err != nil || cfg.SensorOverlap < 0 {
		return cfg, fmt.Errorf("SENSOR_CURSOR_OVERLAP must be a non-negative duration")
	}
	if cfg.SensorStationsRefresh, err = time.ParseDuration(env("SENSOR_STATIONS_REFRESH", "10m")); err != nil || cfg.SensorStationsRefresh <= 0 {
		return cfg, fmt.Errorf("SENSOR_STATIONS_REFRESH must be a positive duration")
	}
	if cfg.SensorMaxFuture, err = time.ParseDuration(env("SENSOR_MAX_FUTURE", "5m")); err != nil || cfg.SensorMaxFuture < 0 {
		return cfg, fmt.Errorf("SENSOR_MAX_FUTURE must be a non-negative duration")
	}
	cfg.SensorPageLimit = envInt("SENSOR_PAGE_LIMIT", 100)
	cfg.SensorMaxPages = envInt("SENSOR_MAX_PAGES", 20)
	cfg.SensorQualityEnum = splitCSV(env("SENSOR_QUALITY_ENUM", "ok,good,fair,poor,degraded,unknown"))
	cfg.SensorSeaMin = envFloat("SENSOR_SEA_LEVEL_MIN", -10)
	cfg.SensorSeaMax = envFloat("SENSOR_SEA_LEVEL_MAX", 10)
	cfg.SensorWaveMin = envFloat("SENSOR_WAVE_MIN", 0)
	cfg.SensorWaveMax = envFloat("SENSOR_WAVE_MAX", 50)
	cfg.SensorSiagaWave = envFloat("SENSOR_SIAGA_WAVE_M", 4)
	cfg.SensorSiagaSea = envFloat("SENSOR_SIAGA_SEA_M", 1.5)
	cfg.SensorWaspadaWave = envFloat("SENSOR_WASPADA_WAVE_M", 2)
	cfg.SensorWaspadaSea = envFloat("SENSOR_WASPADA_SEA_M", 0.75)

	if cfg.BMKGKey == "" || cfg.PVMBGToken == "" {
		return cfg, fmt.Errorf("BMKG_API_KEY and PVMBG_TOKEN are required")
	}
	if cfg.InternalAPIKey == "" {
		return cfg, fmt.Errorf("INTERNAL_API_KEY is required for read API authentication")
	}
	if cfg.SensorURL != "" && cfg.SensorToken == "" {
		return cfg, fmt.Errorf("PANTAI_AUTH_TOKEN is required when SENSOR_URL is set")
	}
	if (cfg.CoordinatorURL == "") != (cfg.StorageURL == "") {
		return cfg, fmt.Errorf("COORDINATOR_URL and STORAGE_URL must be set together for M2 mode")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envFloat(key string, fallback float64) float64 {
	value, err := strconv.ParseFloat(os.Getenv(key), 64)
	if err != nil {
		return fallback
	}
	return value
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func hostnameOr(fallback string) string {
	if name, err := os.Hostname(); err == nil && name != "" {
		return name
	}
	return fallback
}
