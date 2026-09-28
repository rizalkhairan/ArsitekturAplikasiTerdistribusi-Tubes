package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/communication"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/messaging"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/processing"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	// Init db
	db, err := database.Open(cfg.DBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

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

	var workers sync.WaitGroup
	workers.Go(func() { startPoll(ctx, cfg.PollInterval, cfg.KafkaTopic, db, sources) })
	workers.Go(func() {
		messaging.OutboxPublisher{
			DB: db, Publisher: publisher, RetryLimit: cfg.KafkaPublishRetryLimit,
			RetryBackoff: cfg.KafkaPublishRetryBackoff, PollInterval: cfg.OutboxPollInterval,
		}.Run(ctx)
	})
	workers.Go(func() { serveHealth(ctx, cfg.HealthAddress) })
	workers.Wait()
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

func startPoll(ctx context.Context, interval time.Duration, topic string, db *sql.DB, sources *communication.Clients) {
	var pollers sync.WaitGroup
	pollers.Go(func() {
		pollSource(ctx, "BMKG", interval, topic, db, func(ctx context.Context, cursors database.Cursors) (processing.Batch, error) {
			events, err := sources.BMKG.SeismicEvents(ctx, cursors[database.SeismicFeed])
			if err != nil {
				return processing.Batch{}, err
			}
			warnings, err := sources.BMKG.TsunamiWarnings(ctx, cursors[database.TsunamiFeed])
			if err != nil {
				return processing.Batch{}, err
			}
			return processing.AggregateBMKG(ctx, db, events, warnings, cursors)
		})
	})
	pollers.Go(func() {
		pollSource(ctx, "PVMBG", interval, topic, db, func(ctx context.Context, cursors database.Cursors) (processing.Batch, error) {
			reports, err := sources.PVMBG.VolcanicReports(ctx, cursors[database.VolcanicFeed])
			if err != nil {
				return processing.Batch{}, err
			}
			return processing.AggregatePVMBG(ctx, db, reports, cursors)
		})
	})
	pollers.Wait()
}

func pollSource(
	ctx context.Context,
	source string,
	interval time.Duration,
	topic string,
	db *sql.DB,
	fetchAndAggregate func(context.Context, database.Cursors) (processing.Batch, error),
) {
	pollOnce := func() {
		// Load the last committed source timestamps before requesting new records.
		cursors, err := database.LoadCursors(ctx, db)
		var batch processing.Batch
		if err == nil {
			batch, err = fetchAndAggregate(ctx, cursors)
		}

		// Save hazards and advance only this source's cursors in one transaction.
		if err == nil {
			var stored int
			stored, err = database.SaveHazards(ctx, db, batch.Hazards, batch.Cursors, topic)
			if err == nil {
				log.Printf("%s: stored %d canonical events", source, stored)
			}
		}
		if err != nil && ctx.Err() == nil {
			log.Printf("%s poll failed (cursors unchanged): %v", source, err)
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
