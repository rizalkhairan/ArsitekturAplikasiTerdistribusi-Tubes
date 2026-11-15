package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/api"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/communication"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/health"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/messaging"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/processing"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	// Init db
	pools, err := database.OpenPools(cfg.DBPath, cfg.DBReadConnections)
	if err != nil {
		log.Fatal(err)
	}
	defer pools.Close()
	log.Printf("Canonical Store pools: read_connections=%d write_connections=1", cfg.DBReadConnections)

	// Init service comms module
	sources, err := communication.New(
		cfg.BMKGAddress, cfg.BMKGKey,
		cfg.PVMBGURL, cfg.PVMBGToken,
		cfg.UpstreamTimeout,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sources.Close()

	publisher, err := messaging.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaClientID)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	log.Printf("BNPB initialized kafka_topic=%s kafka_client_id=%s", cfg.KafkaTopic, cfg.KafkaClientID)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Track upstream freshness so the read API can mark stale Canonical data.
	registry := health.New()

	// Start HTTP read API for Client-Facing API
	readAPI := api.NewServer(":"+cfg.APIPort, pools.Read, pools.Write, cfg.KafkaTopic, cfg.InternalAPIKey, registry, cfg.StalenessThreshold)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := readAPI.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("BNPB read API error: %v", err)
		}
	}()

	var workers sync.WaitGroup
	workers.Go(func() {
		startPoll(ctx, cfg.PollInterval, cfg.KafkaTopic, pools, sources, registry, cfg.PipelineURL, cfg.PipelineInternalToken)
	})
	workers.Go(func() {
		messaging.OutboxPublisher{
			ReadDB: pools.Read, WriteDB: pools.Write, Publisher: publisher, RetryLimit: cfg.KafkaPublishRetryLimit,
			RetryBackoff: cfg.KafkaPublishRetryBackoff, PollInterval: cfg.OutboxPollInterval,
		}.Run(ctx)
	})
	workers.Go(func() { serveHealth(ctx, cfg.HealthAddress) })
	workers.Wait()

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	readAPI.Shutdown(shutdownCtx)
	wg.Wait()
}

func serveHealth(ctx context.Context, address string) {
	server := &http.Server{
		Addr: address,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/health" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}),
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("health server shutdown failed: %v", err)
		}
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("health server failed: %v", err)
	}
}

func startPoll(ctx context.Context, interval time.Duration, topic string, pools *database.Pools, sources *communication.Clients, registry *health.Registry, pipelineURL, pipelineToken string) {
	var pollers sync.WaitGroup
	pollers.Go(func() {
		pollSource(ctx, "BMKG", interval, topic, pools, registry, "", "", func(ctx context.Context, cursors database.Cursors) (processing.Batch, error) {
			events, err := sources.BMKG.SeismicEvents(ctx, cursors[database.SeismicFeed])
			if err != nil {
				return processing.Batch{}, err
			}
			warnings, err := sources.BMKG.TsunamiWarnings(ctx, cursors[database.TsunamiFeed])
			if err != nil {
				return processing.Batch{}, err
			}
			return processing.AggregateBMKG(ctx, pools.Read, events, warnings, cursors)
		})
	})
	pollers.Go(func() {
		pollSource(ctx, "PVMBG", interval, topic, pools, registry, pipelineURL, pipelineToken, func(ctx context.Context, cursors database.Cursors) (processing.Batch, error) {
			reports, err := sources.PVMBG.VolcanicReports(ctx, cursors[database.VolcanicFeed])
			if err != nil {
				return processing.Batch{}, err
			}
			return processing.AggregatePVMBG(ctx, pools.Read, reports, cursors)
		})
	})
	pollers.Wait()
}

func pollSource(
	ctx context.Context,
	source string,
	interval time.Duration,
	topic string,
	pools *database.Pools,
	registry *health.Registry,
	pipelineURL string,
	pipelineToken string,
	fetchAndAggregate func(context.Context, database.Cursors) (processing.Batch, error),
) {
	pollOnce := func() {
		// Load the last committed source timestamps before requesting new records.
		cursors, err := database.LoadCursors(ctx, pools.Read)
		var batch processing.Batch
		if err == nil {
			batch, err = fetchAndAggregate(ctx, cursors)
		}
		// Quarantine malformed/incompatible PVMBG records durably before advancing the
		// source cursor. Repeated polls are safe because each failure has a stable ID.
		if err == nil && len(batch.Failures) > 0 {
			for _, failure := range batch.Failures {
				if err = parkPVMBGFailure(ctx, pipelineURL, pipelineToken, failure); err != nil {
					break
				}
			}
		}

		// Save hazards and advance only this source's cursors in one transaction.
		if err == nil {
			var stored int
			stored, err = database.SaveHazards(ctx, pools.Write, batch.Hazards, batch.Cursors, topic)
			if err == nil {
				registry.RecordSuccess(source)
				log.Printf("%s: stored %d canonical events", source, stored)
			}
		}
		if err != nil {
			registry.RecordFailure(source, err)
			if ctx.Err() == nil {
				log.Printf("%s poll failed (cursors unchanged): %v", source, err)
			}
		}
	}

	// The ticker sends a signal every POLL_INTERVAL (default: 3 seconds).
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Printf("%s: polling immediately, then every %s", source, interval)

	pollOnce() // Load existing records immediately at startup.

	for {
		select {
		case <-ticker.C:
			// Run fetch → aggregate → store again on each tick. Calls for this source never overlap;
			// if a poll takes longer than the interval, the ticker drops missed ticks.
			pollOnce()
		case <-ctx.Done():
			return
		}
	}
}

func parkPVMBGFailure(ctx context.Context, baseURL, token string, failure processing.FailedMessage) error {
	if strings.TrimSpace(baseURL) == "" || strings.TrimSpace(token) == "" {
		return fmt.Errorf("PVMBG schema failure cannot be skipped: INGEST_PIPELINE_URL and PIPELINE_INTERNAL_TOKEN are required")
	}
	h := sha256.New()
	h.Write([]byte("PVMBG"))
	h.Write([]byte{0})
	h.Write([]byte(failure.SourceRefID))
	h.Write([]byte{0})
	h.Write(failure.Raw)
	messageID := hex.EncodeToString(h.Sum(nil))
	body, err := json.Marshal(map[string]any{"message_id": messageID, "source": "PVMBG", "source_ref_id": failure.SourceRefID, "received_at": failure.ReceivedAt, "reason": failure.Reason, "retryable": failure.Retryable, "attempts": 1, "raw_base64": base64.StdEncoding.EncodeToString(failure.Raw)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/internal/dlq/park", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("persist PVMBG failure %s: %w", failure.SourceRefID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("persist PVMBG failure %s: ingest pipeline returned HTTP %d", failure.SourceRefID, resp.StatusCode)
	}
	return nil
}
