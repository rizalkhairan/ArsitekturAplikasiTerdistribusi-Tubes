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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/api"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/communication"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/coordclient"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/dlqclient"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/health"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/leader"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/leaderpoll"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/messaging"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/processing"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/sensor"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/storeclient"
	"github.com/rizalkhairan/distributed-system/observability"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	pools, err := database.OpenPools(cfg.DBPath, cfg.DBReadConnections)
	if err != nil {
		log.Fatal(err)
	}
	defer pools.Close()
	log.Printf("Canonical Store pools: read_connections=%d write_connections=1", cfg.DBReadConnections)

	sources, err := communication.New(
		cfg.BMKGAddress, cfg.BMKGKey,
		cfg.PVMBGURL, cfg.PVMBGToken,
		cfg.UpstreamTimeout,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sources.Close()

	logger := obs.NewLogger("aggregator")
	obs.SetGlobalLogger(logger)
	obs.EnableOTLP(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "aggregator")
	registryMetrics := obs.NewRegistry("aggregator")
	httpMetrics := obs.NewHTTPMiddleware("aggregator", logger, registryMetrics)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	registry := health.New()

	m2 := cfg.CoordinatorURL != "" && cfg.StorageURL != ""
	var coord *coordclient.Client
	var store *storeclient.Client
	var dlq *dlqclient.Client
	if m2 {
		coord = coordclient.New(cfg.CoordinatorURL, cfg.CoordinatorToken, cfg.UpstreamTimeout)
		store = storeclient.New(cfg.StorageURL, cfg.StorageToken, cfg.InstanceID, cfg.UpstreamTimeout)
		if cfg.DLQURL != "" {
			dlq = dlqclient.New(cfg.DLQURL, cfg.DLQToken, cfg.UpstreamTimeout)
			dlq.SetAuditToken(cfg.AuditToken)
		}
		logger.Info(ctx, "m2_mode_enabled", "coordinator", cfg.CoordinatorURL, "canonical", cfg.StorageURL,
			"instance", cfg.InstanceID, "kafka_enabled", cfg.KafkaEnabled, "mirror_local", cfg.MirrorLocal)
	}

	var sensorBreaker *sensor.Breaker
	var sensorCache *sensor.Cache
	var sensorProgress *sensor.Progress
	var sensorPoller *sensor.Poller
	var sensorDeliverer *sensor.Deliverer
	sensorMetrics := sensor.NewMetrics(registryMetrics)
	if cfg.SensorURL != "" && m2 {
		sensorCache = sensor.NewCache()
		sensorProgress = sensor.NewProgress()
		sensorBreaker = sensor.NewBreaker(sensor.DefaultBreakerConfig())
		sensorBreaker.OnState = func(state sensor.BreakerState, reason string) {
			sensorMetrics.SetBreaker(state)
			logger.Warn(ctx, "sensor_breaker", "state", string(state), "reason", reason)
		}
		client := sensor.NewClient(sensor.ClientConfig{
			BaseURL: cfg.SensorURL, Token: cfg.SensorToken, Timeout: cfg.SensorTimeout,
			MaxAttempts: 3, RetryBudget: 10, RetryBudgetPerSecond: 0.2, SlowThreshold: 2 * time.Second,
		}, sensorBreaker, sensorMetrics, logger)
		pollerCfg := sensor.DefaultConfig()
		pollerCfg.PollInterval = cfg.SensorPollInterval
		pollerCfg.StaleAfter = cfg.SensorStaleAfter
		pollerCfg.PageLimit = cfg.SensorPageLimit
		pollerCfg.MaxPages = cfg.SensorMaxPages
		pollerCfg.Overlap = cfg.SensorOverlap
		pollerCfg.StationsRefresh = cfg.SensorStationsRefresh
		pollerCfg.Validation = sensor.ValidationConfig{
			SeaLevelMin: cfg.SensorSeaMin, SeaLevelMax: cfg.SensorSeaMax,
			WaveMin: cfg.SensorWaveMin, WaveMax: cfg.SensorWaveMax,
			MaxFuture: cfg.SensorMaxFuture, MaxAge: 0,
			QualityEnum: qualityEnum(cfg.SensorQualityEnum), RequireUTC: true,
		}
		pollerCfg.Severity = sensor.SeverityConfig{
			SiagaWave: cfg.SensorSiagaWave, SiagaSea: cfg.SensorSiagaSea,
			WaspadaWave: cfg.SensorWaspadaWave, WaspadaSea: cfg.SensorWaspadaSea,
		}
		sensorPoller = sensor.NewPoller(pollerCfg, client, coord, store, dlq, sensorBreaker,
			sensorCache, sensorProgress, sensorMetrics, logger, cfg.InstanceID)
		sensorPoller.RunParkWorkers(ctx)
		if cfg.NotifierURL != "" {
			sensorDeliverer = sensor.NewDeliverer(coord, cfg.NotifierURL, cfg.NotifierToken, cfg.InstanceID, sensorMetrics, logger)
		}
	}

	readAPI := api.NewServer(":"+cfg.APIPort, pools.Read, pools.Write, cfg.KafkaTopic, cfg.InternalAPIKey, registry, cfg.StalenessThreshold)
	readAPI.SetReadCacheTTL(cfg.ReadCacheTTL)
	readAPI.SetMetrics(registryMetrics.Handler())
	readAPI.ConfigureObservability(httpMetrics)
	if m2 {
		readAPI.ConfigureM2(store, dlq, cfg.AuditToken, freshnessFunc(ctx, coord, store, cfg), func(ctx context.Context) any {
			return sensorStatus(ctx, store, sensorBreaker, sensorCache, sensorProgress, cfg)
		})
	}
	if sensorCache != nil {
		severityCfg := sensor.SeverityConfig{
			SiagaWave: cfg.SensorSiagaWave, SiagaSea: cfg.SensorSiagaSea,
			WaspadaWave: cfg.SensorWaspadaWave, WaspadaSea: cfg.SensorWaspadaSea,
		}
		readAPI.SetSensorFallback(func() ([]database.HazardEvent, time.Time, bool) {
			observations, asOf := sensorCache.Snapshot()
			stations, _ := sensorCache.Stations()
			events := make([]database.HazardEvent, 0, len(observations))
			for _, obs := range observations {
				station, ok := stations[obs.StationID]
				if !ok {
					continue
				}
				events = append(events, sensor.ToHazardEvent(obs, station, sensor.Severity(obs, severityCfg)))
			}
			return events, asOf, len(events) > 0
		})
	}

	var ready atomic.Bool
	ready.Store(true)
	readAPI.SetReady(func() bool { return ready.Load() })

	var apiWG sync.WaitGroup
	apiWG.Add(1)
	go func() {
		defer apiWG.Done()
		if err := readAPI.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("BNPB read API error: %v", err)
		}
	}()

	var workers sync.WaitGroup
	var publisher messaging.Publisher
	if cfg.KafkaEnabled {
		kafkaPublisher, err := messaging.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaClientID)
		if err != nil {
			log.Fatal(err)
		}
		publisher = kafkaPublisher
		defer publisher.Close()
	}

	if m2 {
		leaderMetrics := leader.NewMetrics(registryMetrics)
		pollers := leaderpoll.New(leaderpoll.Config{
			PollInterval: cfg.PollInterval, MirrorLocal: cfg.MirrorLocal, KafkaTopic: cfg.KafkaTopic,
			Holder: cfg.InstanceID,
		}, sources, coord, store, dlq, pools, registry, logger)

		startLeader(ctx, &workers, leader.Runner{Source: "BMKG", Holder: cfg.InstanceID, Coord: coord,
			TTL: cfg.LeaseTTL, RenewInterval: cfg.LeaseRenew, SafetyMargin: cfg.LeaseSafety,
			Metrics: leaderMetrics, Logger: logger}, pollers.RunBMKG)
		startLeader(ctx, &workers, leader.Runner{Source: "PVMBG", Holder: cfg.InstanceID, Coord: coord,
			TTL: cfg.LeaseTTL, RenewInterval: cfg.LeaseRenew, SafetyMargin: cfg.LeaseSafety,
			Metrics: leaderMetrics, Logger: logger}, pollers.RunPVMBG)
		if sensorPoller != nil {
			startLeader(ctx, &workers, leader.Runner{Source: sensor.Source, Holder: cfg.InstanceID, Coord: coord,
				TTL: cfg.LeaseTTL, RenewInterval: cfg.LeaseRenew, SafetyMargin: cfg.LeaseSafety,
				Metrics: leaderMetrics, Logger: logger}, func(ctx context.Context, lease coordclient.Lease) error {
				sensorPoller.RunLeader(ctx, lease)
				return nil
			})
		}
		if sensorDeliverer != nil {
			workers.Go(func() { sensorDeliverer.Run(ctx) })
		}
		if cfg.KafkaEnabled && cfg.MirrorLocal {
			workers.Go(func() {
				messaging.OutboxPublisher{
					ReadDB: pools.Read, WriteDB: pools.Write, Publisher: publisher,
					RetryLimit: cfg.KafkaPublishRetryLimit, RetryBackoff: cfg.KafkaPublishRetryBackoff,
					PollInterval: cfg.OutboxPollInterval,
				}.Run(ctx)
			})
		}
	} else {
		workers.Go(func() {
			startPoll(ctx, cfg.PollInterval, cfg.KafkaTopic, pools, sources, registry, cfg.PipelineURL, cfg.PipelineInternalToken)
		})
		if cfg.KafkaEnabled {
			workers.Go(func() {
				messaging.OutboxPublisher{
					ReadDB: pools.Read, WriteDB: pools.Write, Publisher: publisher,
					RetryLimit: cfg.KafkaPublishRetryLimit, RetryBackoff: cfg.KafkaPublishRetryBackoff,
					PollInterval: cfg.OutboxPollInterval,
				}.Run(ctx)
			})
		}
	}

	workers.Go(func() { serveHealth(ctx, cfg.HealthAddress) })

	<-ctx.Done()
	ready.Store(false)
	drain := envDuration("SHUTDOWN_DRAIN", 5*time.Second)
	logger.Info(context.Background(), "shutdown_started", "drain", drain.String())
	time.Sleep(drain)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := readAPI.Shutdown(shutdownCtx); err != nil {
		logger.Warn(context.Background(), "read_api_shutdown_failed", "error", err.Error())
	}
	workers.Wait()
	apiWG.Wait()
	logger.Info(context.Background(), "shutdown_complete")
}

func startLeader(ctx context.Context, wg *sync.WaitGroup, runner leader.Runner,
	work func(context.Context, coordclient.Lease) error) {
	wg.Go(func() { runner.Run(ctx, work) })
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
		_ = server.Shutdown(shutdownCtx)
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
	ctx context.Context, source string, interval time.Duration, topic string,
	pools *database.Pools, registry *health.Registry, pipelineURL, pipelineToken string,
	fetchAndAggregate func(context.Context, database.Cursors) (processing.Batch, error),
) {
	pollOnce := func() {
		cursors, err := database.LoadCursors(ctx, pools.Read)
		var batch processing.Batch
		if err == nil {
			batch, err = fetchAndAggregate(ctx, cursors)
		}
		if err == nil && len(batch.Failures) > 0 {
			for _, failure := range batch.Failures {
				if err = parkPVMBGFailure(ctx, pipelineURL, pipelineToken, failure); err != nil {
					break
				}
			}
		}
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
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Printf("%s: polling immediately, then every %s", source, interval)
	pollOnce()
	for {
		select {
		case <-ticker.C:
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

type freshnessEntry struct {
	last  time.Time
	stale bool
	msg   string
	at    time.Time
}

type freshnessCache struct {
	mu       sync.Mutex
	entries  map[string]freshnessEntry
	inflight map[string]bool
	coord    *coordclient.Client
	store    *storeclient.Client
	cfg      config
}

// freshnessFunc keeps coordinator/storage latency out of the request path.
// Every source is refreshed in the background; handlers only read the last
// snapshot (stale data is preferable to blocking every /hazards request).
func freshnessFunc(ctx context.Context, coord *coordclient.Client, store *storeclient.Client, cfg config) api.FreshnessFunc {
	f := &freshnessCache{
		entries: map[string]freshnessEntry{}, inflight: map[string]bool{},
		coord: coord, store: store, cfg: cfg,
	}
	go f.loop(ctx)
	return f.lookup
}

func (f *freshnessCache) lookup(_ context.Context, source string) (time.Time, bool, string) {
	f.mu.Lock()
	entry, ok := f.entries[source]
	if !ok || time.Since(entry.at) >= 2*time.Second {
		if !f.inflight[source] {
			f.inflight[source] = true
			go f.refresh(source)
		}
	}
	f.mu.Unlock()
	if !ok {
		return time.Time{}, true, "freshness warming"
	}
	return entry.last, entry.stale, entry.msg
}

func (f *freshnessCache) loop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	sources := []string{"BMKG", "PVMBG", sensor.Source}
	for {
		for _, source := range sources {
			f.mu.Lock()
			if f.inflight[source] {
				f.mu.Unlock()
				continue
			}
			f.inflight[source] = true
			f.mu.Unlock()
			go f.refresh(source)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (f *freshnessCache) refresh(source string) {
	defer func() {
		f.mu.Lock()
		delete(f.inflight, source)
		f.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	last, stale, msg := lookupFreshness(ctx, f.coord, f.store, f.cfg, source)
	f.mu.Lock()
	f.entries[source] = freshnessEntry{last: last, stale: stale, msg: msg, at: time.Now()}
	f.mu.Unlock()
}

func lookupFreshness(ctx context.Context, coord *coordclient.Client, store *storeclient.Client, cfg config, source string) (time.Time, bool, string) {
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if source == sensor.Source {
		events, err := store.List(callCtx, source, "", 1)
		if err != nil || len(events) == 0 {
			return time.Time{}, true, "no sensor data available"
		}
		latest := events[0].OccurredAt
		return latest, time.Since(latest) > cfg.SensorStaleAfter, "progress-based sensor freshness"
	}
	checkpoint, err := coord.Checkpoint(callCtx, source)
	if err != nil {
		return time.Time{}, true, err.Error()
	}
	last, err := time.Parse(time.RFC3339Nano, checkpoint.UpdatedAt)
	if err != nil {
		return time.Time{}, true, "no committed checkpoint"
	}
	return last, time.Since(last) > cfg.StalenessThreshold, "last committed poll"
}

func sensorStatus(ctx context.Context, store *storeclient.Client, breaker *sensor.Breaker,
	cache *sensor.Cache, progress *sensor.Progress, cfg config) any {
	response := map[string]any{"source": sensor.Source, "stale": true}
	if cache != nil {
		observations, asOf := cache.Snapshot()
		if !asOf.IsZero() {
			response["as_of"] = asOf.UTC().Format(time.RFC3339Nano)
			response["stale"] = time.Since(asOf) > cfg.SensorStaleAfter
		}
		response["stations"] = len(observations)
		response["observations"] = observations
	}
	if breaker != nil {
		state, reason := breaker.State()
		response["breaker"] = string(state)
		response["breaker_reason"] = reason
	}
	if progress != nil {
		lastFresh, lastObserved, lastGenerated, seen := progress.Snapshot()
		if !lastFresh.IsZero() {
			response["last_fresh_at"] = lastFresh.UTC().Format(time.RFC3339Nano)
			response["seconds_since_fresh"] = time.Since(lastFresh).Seconds()
		}
		if !lastObserved.IsZero() {
			response["last_observed_at"] = lastObserved.UTC().Format(time.RFC3339Nano)
		}
		if !lastGenerated.IsZero() {
			response["last_generated_at"] = lastGenerated.UTC().Format(time.RFC3339Nano)
		}
		response["seen_observations"] = seen
	}
	if cache == nil || response["as_of"] == nil {
		callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if events, err := store.List(callCtx, sensor.Source, "", 1); err == nil && len(events) > 0 {
			response["as_of"] = events[0].OccurredAt.UTC().Format(time.RFC3339Nano)
			response["stale"] = time.Since(events[0].OccurredAt) > cfg.SensorStaleAfter
		}
	}
	return response
}

func qualityEnum(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
