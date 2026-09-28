package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const consumerName = "dashboard-updater"

type hazardEvent struct {
	EventID       string          `json:"event_id"`
	HazardID      string          `json:"hazard_id"`
	Source        string          `json:"source"`
	HazardType    string          `json:"hazard_type"`
	Severity      string          `json:"severity"`
	AreaName      string          `json:"area_name"`
	CorrelationID string          `json:"correlation_id"`
	Attributes    json.RawMessage `json:"attributes"`
}

type deduper struct {
	mu    sync.Mutex
	limit int
	order []string
	seen  map[string]struct{}
}

func newDeduper(limit int) *deduper {
	return &deduper{limit: limit, seen: make(map[string]struct{}, limit)}
}

func (d *deduper) addIfNew(eventID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.seen[eventID]; exists {
		return false
	}
	d.seen[eventID] = struct{}{}
	d.order = append(d.order, eventID)
	if len(d.order) > d.limit {
		delete(d.seen, d.order[0])
		d.order = d.order[1:]
	}
	return true
}

func validateEvent(payload []byte) (hazardEvent, error) {
	var event hazardEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return event, fmt.Errorf("decode hazard event: %w", err)
	}
	for field, value := range map[string]string{
		"event_id": event.EventID, "hazard_id": event.HazardID, "source": event.Source,
		"hazard_type": event.HazardType, "severity": event.Severity, "area_name": event.AreaName,
		"correlation_id": event.CorrelationID,
	} {
		if strings.TrimSpace(value) == "" {
			return event, fmt.Errorf("hazard event %s is required", field)
		}
	}
	if !json.Valid(event.Attributes) {
		return event, fmt.Errorf("hazard event attributes must be valid JSON")
	}
	return event, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var healthy atomic.Bool
	go serveHealth(ctx, env("HEALTH_ADDR", ":8080"), &healthy)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")...),
		kgo.ConsumerGroup("dashboard-updater-group"),
		kgo.ConsumeTopics(env("KAFKA_TOPIC_HAZARD_EVENTS", "hazard-events")),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Printf("consumer initialization failed consumer=%s error=%v", consumerName, err)
		<-ctx.Done()
		return
	}
	defer client.Close()
	healthy.Store(true)
	run(ctx, client, newDeduper(envInt("IDEMPOTENCY_CACHE_SIZE", 10000)), &healthy)
}

func run(ctx context.Context, client *kgo.Client, processed *deduper, healthy *atomic.Bool) {
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			healthy.Store(false)
			if ctx.Err() == nil {
				log.Printf("consumer poll failed consumer=%s result=error error=%v", consumerName, err)
			}
			continue
		}
		healthy.Store(true)
		fetches.EachRecord(func(record *kgo.Record) {
			if err := handleRecord(ctx, client, record, processed); err != nil && ctx.Err() == nil {
				log.Printf("consumer record left uncommitted consumer=%s topic=%s partition=%d offset=%d result=retry error=%v",
					consumerName, record.Topic, record.Partition, record.Offset, err)
			}
		})
	}
}

func handleRecord(ctx context.Context, client *kgo.Client, record *kgo.Record, processed *deduper) error {
	event, err := validateEvent(record.Value)
	if err == nil && processed.addIfNew(event.EventID) {
		log.Printf("dashboard updated consumer=%s hazard_id=%s event_id=%s topic=%s partition=%d offset=%d correlation_id=%s result=success",
			consumerName, event.HazardID, event.EventID, record.Topic, record.Partition, record.Offset, event.CorrelationID)
	} else if err == nil {
		log.Printf("dashboard update skipped consumer=%s hazard_id=%s event_id=%s topic=%s partition=%d offset=%d correlation_id=%s result=duplicate",
			consumerName, event.HazardID, event.EventID, record.Topic, record.Partition, record.Offset, event.CorrelationID)
	}
	if err == nil {
		return commit(ctx, client, record)
	}
	for attempt := 1; attempt <= envInt("PROCESS_RETRY_LIMIT", 3); attempt++ {
		time.Sleep(parseDuration("PROCESS_RETRY_BACKOFF", "500ms"))
		event, err = validateEvent(record.Value)
		if err == nil {
			return commit(ctx, client, record)
		}
	}
	if err := publishDLQ(ctx, client, record, err); err != nil {
		return err
	}
	return commit(ctx, client, record)
}

func commit(ctx context.Context, client *kgo.Client, record *kgo.Record) error {
	return client.CommitRecords(ctx, record)
}

func publishDLQ(ctx context.Context, client *kgo.Client, record *kgo.Record, processingErr error) error {
	payload, err := json.Marshal(map[string]any{
		"original_event": json.RawMessage(record.Value), "consumer": consumerName,
		"failure": processingErr.Error(), "failed_at": time.Now().UTC().Format(time.RFC3339Nano),
		"source_topic": record.Topic, "partition": record.Partition, "offset": record.Offset,
		"attempts": envInt("PROCESS_RETRY_LIMIT", 3),
	})
	if err != nil {
		return err
	}
	return client.ProduceSync(ctx, &kgo.Record{Topic: env("KAFKA_DLQ_TOPIC", "hazard-events-dlq"), Key: record.Key, Value: payload}).FirstErr()
}

func serveHealth(ctx context.Context, address string, healthy *atomic.Bool) {
	server := &http.Server{Addr: address, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"unhealthy","reason":"kafka consumer loop is not initialized"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})}
	go func() { <-ctx.Done(); _ = server.Shutdown(context.Background()) }()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("health server failed consumer=%s error=%v", consumerName, err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	var value int
	if _, err := fmt.Sscanf(env(key, fmt.Sprint(fallback)), "%d", &value); err != nil || value < 1 {
		return fallback
	}
	return value
}

func parseDuration(key, fallback string) time.Duration {
	value, err := time.ParseDuration(env(key, fallback))
	if err != nil || value < 0 {
		return time.Second
	}
	return value
}
