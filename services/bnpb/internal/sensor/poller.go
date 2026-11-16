package sensor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/coordclient"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/dlqclient"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/storeclient"
	"github.com/rizalkhairan/distributed-system/observability"
)

type Config struct {
	PollInterval    time.Duration
	PageLimit       int
	MaxPages        int
	Overlap         time.Duration
	StaleAfter      time.Duration
	StationsRefresh time.Duration
	ParkQueueSize   int
	ParkWorkers     int
	Validation      ValidationConfig
	Severity        SeverityConfig
}

func DefaultConfig() Config {
	return Config{
		PollInterval: 2 * time.Second, PageLimit: 100, MaxPages: 20,
		Overlap: 5 * time.Minute, StaleAfter: 45 * time.Second,
		StationsRefresh: 10 * time.Minute, ParkQueueSize: 512, ParkWorkers: 4,
		Validation: DefaultValidationConfig(), Severity: DefaultSeverityConfig(),
	}
}

type Poller struct {
	cfg       Config
	client    *Client
	coord     *coordclient.Client
	store     *storeclient.Client
	dlq       *dlqclient.Client
	breaker   *Breaker
	cache     *Cache
	progress  *Progress
	metrics   *Metrics
	logger    *obs.Logger
	instance  string
	parkQueue chan parkJob
}

type parkJob struct {
	raw    json.RawMessage
	reason string
	detail string
}

func NewPoller(cfg Config, client *Client, coord *coordclient.Client, store *storeclient.Client,
	dlq *dlqclient.Client, breaker *Breaker, cache *Cache, progress *Progress,
	metrics *Metrics, logger *obs.Logger, instance string) *Poller {
	if cfg.ParkQueueSize <= 0 {
		cfg.ParkQueueSize = 512
	}
	if cfg.ParkWorkers <= 0 {
		cfg.ParkWorkers = 4
	}
	return &Poller{cfg: cfg, client: client, coord: coord, store: store, dlq: dlq,
		breaker: breaker, cache: cache, progress: progress, metrics: metrics, logger: logger,
		instance: instance, parkQueue: make(chan parkJob, cfg.ParkQueueSize)}
}

type cursorState struct {
	Stations map[string]stationCursor `json:"stations"`
}

type stationCursor struct {
	Cursor         string `json:"cursor"`
	LastObservedAt string `json:"last_observed_at"`
}

func (p *Poller) RunParkWorkers(ctx context.Context) {
	for i := 0; i < p.cfg.ParkWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case job := <-p.parkQueue:
					p.parkSync(ctx, job)
				}
			}
		}()
	}
}

func (p *Poller) RunLeader(ctx context.Context, lease coordclient.Lease) {
	if p.metrics != nil {
		p.metrics.SetBreaker(Closed)
	}
	var state cursorState
	checkpoint, err := p.coord.Checkpoint(ctx, Source)
	if err == nil && checkpoint.Cursor != "" {
		if err := json.Unmarshal([]byte(checkpoint.Cursor), &state); err != nil {
			p.logger.Warn(ctx, "sensor_checkpoint_decode_failed", "error", err.Error())
		}
	}
	if state.Stations == nil {
		state.Stations = map[string]stationCursor{}
	}
	p.logger.Info(ctx, "sensor_leader_started", "epoch", lease.Epoch, "instance", p.instance)

	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := p.pollOnce(ctx, lease, &state); err != nil {
			if errors.Is(err, coordclient.ErrStaleEpoch) || errors.Is(err, storeclient.ErrFenced) {
				p.logger.Warn(ctx, "sensor_leader_fenced", "epoch", lease.Epoch)
				return
			}
			if ctx.Err() != nil {
				return
			}
			p.logger.Warn(ctx, "sensor_poll_failed", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context, lease coordclient.Lease, state *cursorState) error {
	ctx, span := obs.StartSpan(ctx, "poll.sensor")
	defer span.End(ctx)
	span.Set("epoch", lease.Epoch)
	if err := p.coord.Validate(ctx, Source, p.instance, lease.Epoch); err != nil {
		return err
	}
	stations, stationAt := p.cache.Stations()
	if len(stations) == 0 || time.Since(stationAt) > p.cfg.StationsRefresh {
		fresh, err := p.client.Stations(ctx)
		if err != nil {
			if errors.Is(err, ErrCircuitOpen) || errors.Is(err, ErrPermanent) {
				p.observeHealth()
				return nil
			}
			if len(stations) == 0 {
				return fmt.Errorf("load stations: %w", err)
			}
			p.logger.Warn(ctx, "sensor_station_refresh_failed", "error", err.Error())
		} else if len(fresh) > 0 {
			p.cache.SetStations(fresh)
			stations, _ = p.cache.Stations()
		}
	}
	if len(stations) == 0 {
		return errors.New("no sensor stations available")
	}

	ids := make([]string, 0, len(stations))
	for id := range stations {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	changed := false
	var failures []error
	for _, id := range ids {
		cursor := state.Stations[id]
		updated, err := p.pollStation(ctx, lease, stations[id], stations, cursor)
		if err != nil {
			if errors.Is(err, coordclient.ErrStaleEpoch) || errors.Is(err, storeclient.ErrFenced) {
				return err
			}
			failures = append(failures, fmt.Errorf("%s: %w", id, err))
			continue
		}
		if updated != cursor {
			state.Stations[id] = updated
			changed = true
		}
	}
	if changed {
		raw, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if _, err := p.coord.CommitCheckpoint(ctx, Source, lease.Epoch, string(raw)); err != nil {
			if errors.Is(err, coordclient.ErrStaleEpoch) {
				return coordclient.ErrStaleEpoch
			}
			return fmt.Errorf("commit sensor checkpoint: %w", err)
		}
	}
	p.observeHealth()
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	return nil
}

func (p *Poller) pollStation(ctx context.Context, lease coordclient.Lease, station Station, stations map[string]Station, state stationCursor) (stationCursor, error) {
	cursor := state.Cursor
	lastObserved, _ := time.Parse(time.RFC3339Nano, state.LastObservedAt)
	fromStart := state.Cursor == ""
	retried := false
	for pageNo := 0; pageNo < p.cfg.MaxPages; pageNo++ {
		if err := p.coord.Validate(ctx, Source, p.instance, lease.Epoch); err != nil {
			return state, err
		}
		page, err := p.client.Observations(ctx, station.StationID, cursor, p.cfg.PageLimit)
		if err != nil {
			if errors.Is(err, ErrCursorInvalid) && !retried {
				// Sensor restarted and its /data (emptyDir) lost the cursor. Re-read
				// from the beginning, but only keep observations newer than the last
				// committed observed_at minus an overlap window.
				p.logger.Warn(ctx, "sensor_cursor_invalid", "station_id", station.StationID, "error", err.Error())
				cursor = ""
				fromStart = true
				retried = true
				continue
			}
			if errors.Is(err, ErrCircuitOpen) {
				return state, nil
			}
			return state, err
		}
		generatedAt, _ := time.Parse(time.RFC3339Nano, page.GeneratedAt)
		for _, raw := range page.Items {
			obs, violations := Validate(raw, p.cfg.Validation, stations, time.Now())
			if len(violations) > 0 {
				p.enqueuePark(raw, violations[0])
				continue
			}
			if fromStart && !lastObserved.IsZero() && obs.ObservedAt.Before(lastObserved.Add(-p.cfg.Overlap)) {
				continue
			}
			key := CompositeKey(obs)
			fresh := p.progress.Observe(key, obs.ObservedAt, generatedAt)
			if fresh && p.metrics != nil {
				p.metrics.LastFresh.WithLabelValues(Source).Set(0)
			}
			severity := Severity(obs, p.cfg.Severity)
			if obs.Critical {
				payload := p.notificationPayload(obs, station, severity)
				if _, _, err := p.coord.Enqueue(ctx, key, payload); err != nil {
					return state, fmt.Errorf("critical outbox enqueue: %w", err)
				}
				if p.metrics != nil {
					p.metrics.OutboxEnqueued.WithLabelValues(Source).Inc()
				}
			}
			event := ToHazardEvent(obs, station, severity)
			if _, _, err := p.store.Upsert(ctx, event, 1, lease.Epoch, Source); err != nil {
				if errors.Is(err, storeclient.ErrFenced) {
					if p.metrics != nil {
						p.metrics.Fenced.WithLabelValues(Source).Inc()
					}
					return state, storeclient.ErrFenced
				}
				return state, fmt.Errorf("store observation %s: %w", key, err)
			}
			p.cache.Put(obs)
			if obs.ObservedAt.After(lastObserved) {
				lastObserved = obs.ObservedAt
			}
		}
		next := page.NextCursor
		if next == "" || next == cursor {
			cursor = next
			break
		}
		cursor = next
		fromStart = false
	}
	updated := stationCursor{Cursor: cursor}
	if !lastObserved.IsZero() {
		updated.LastObservedAt = lastObserved.UTC().Format(time.RFC3339Nano)
	}
	return updated, nil
}

func (p *Poller) observeHealth() {
	since := p.progress.SinceFresh(time.Now())
	if p.metrics != nil {
		if since < time.Hour {
			p.metrics.LastFresh.WithLabelValues(Source).Set(since.Seconds())
		}
	}
	if since > p.cfg.StaleAfter {
		p.breaker.Trip("staleness")
		if p.logger != nil {
			p.logger.Warn(context.Background(), "sensor_stale", "seconds_since_fresh", since.Seconds())
		}
	}
	if p.metrics != nil {
		state, _ := p.breaker.State()
		p.metrics.SetBreaker(state)
	}
}

func (p *Poller) enqueuePark(raw json.RawMessage, violation Violation) {
	job := parkJob{raw: raw, reason: violation.Code, detail: violation.Detail}
	select {
	case p.parkQueue <- job:
		if p.metrics != nil {
			p.metrics.QueueDepth.WithLabelValues("dlq").Set(float64(len(p.parkQueue)))
		}
	default:
		// Garbage storms must not block valid observations or critical alerts.
		if p.metrics != nil {
			p.metrics.DLQParked.WithLabelValues(Source, "overflow").Inc()
		}
		log.Printf(`{"level":"warn","service":"sensor","msg":"dlq_queue_full","reason":%q}`, violation.Code)
	}
}

func (p *Poller) parkSync(ctx context.Context, job parkJob) {
	if p.dlq == nil {
		if p.metrics != nil {
			p.metrics.DLQParked.WithLabelValues(Source, "dlq_unconfigured").Inc()
		}
		return
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := p.dlq.Park(timeoutCtx, Source, job.raw, job.reason, false); err != nil {
		if p.metrics != nil {
			p.metrics.DLQParked.WithLabelValues(Source, "park_failed").Inc()
		}
		return
	}
	if p.metrics != nil {
		p.metrics.DLQParked.WithLabelValues(Source, job.reason).Inc()
	}
	if p.logger != nil {
		p.logger.Info(timeoutCtx, "sensor_parked", "reason", job.reason, "detail", job.detail)
	}
}

func (p *Poller) notificationPayload(obs Observation, station Station, severity string) map[string]any {
	return map[string]any{
		"dedupe_key": CompositeKey(obs), "event_id": obs.ObservationID,
		"source": Source, "station_id": obs.StationID,
		"observed_at":   obs.ObservedAt.UTC().Format(time.RFC3339Nano),
		"dispatched_at": time.Now().UTC().Format(time.RFC3339Nano),
		"severity":      severity, "message": "critical coastal observation",
		"attributes": map[string]any{
			"sea_level_anomaly_m": obs.SeaLevelAnomaly, "wave_height_m": obs.WaveHeight,
			"quality": obs.Quality, "critical": true, "station_name": station.Name,
			"province": station.Province,
		},
	}
}

func ToHazardEvent(obs Observation, station Station, severity string) database.HazardEvent {
	area := station.Province
	if area == "" {
		area = station.Name
	}
	return database.HazardEvent{
		Source: Source, SourceRefID: CompositeKey(obs), HazardType: "COASTAL",
		Severity: severity, AreaName: area, Latitude: station.Latitude, Longitude: station.Longitude,
		OccurredAt: obs.ObservedAt,
		Attributes: map[string]any{
			"station_id": obs.StationID, "observation_id": obs.ObservationID,
			"sea_level_anomaly_m": obs.SeaLevelAnomaly, "wave_height_m": obs.WaveHeight,
			"quality": obs.Quality, "critical": obs.Critical,
		},
	}
}

// jitter returns a small positive jitter so three replicas do not synchronize.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(d/10) + 1))
}
