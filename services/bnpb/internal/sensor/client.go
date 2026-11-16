package sensor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rizalkhairan/distributed-system/observability"
)

var (
	ErrCircuitOpen   = errors.New("sensor circuit breaker is open")
	ErrCursorInvalid = errors.New("sensor cursor is no longer valid")
	ErrPermanent     = errors.New("sensor returned a permanent error")
)

type Metrics struct {
	BreakerState    *obs.GaugeVec
	Requests        *obs.CounterVec
	Duration        *obs.HistogramVec
	LastFresh       *obs.GaugeVec
	QueueDepth      *obs.GaugeVec
	DLQParked       *obs.CounterVec
	OutboxEnqueued  *obs.CounterVec
	OutboxDelivered *obs.CounterVec
	AlertLatency    *obs.HistogramVec
	Fenced          *obs.CounterVec
	CacheStale      *obs.GaugeVec
	Retries         *obs.CounterVec
}

func NewMetrics(registry *obs.Registry) *Metrics {
	return &Metrics{
		BreakerState: registry.Gauge("sensor_breaker_state", "0=closed 1=half_open 2=open", "source"),
		Requests:     registry.Counter("sensor_requests_total", "Sensor requests by operation and outcome", "source", "operation", "outcome"),
		Duration: registry.Histogram("sensor_request_duration_seconds", "Sensor request latency",
			[]float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10}, "source", "operation"),
		LastFresh:       registry.Gauge("sensor_seconds_since_last_fresh_observation", "Progress-based sensor freshness", "source"),
		QueueDepth:      registry.Gauge("sensor_queue_depth", "Bounded sensor queue depth", "queue"),
		DLQParked:       registry.Counter("dlq_parked_total", "Invalid sensor observations parked", "source", "reason"),
		OutboxEnqueued:  registry.Counter("critical_outbox_enqueued_total", "Critical notifications durably enqueued", "source"),
		OutboxDelivered: registry.Counter("critical_outbox_delivered_total", "Critical notifications delivered", "source", "outcome"),
		AlertLatency: registry.Histogram("critical_alert_end_to_end_seconds", "Critical alert observed_at to notifier receipt",
			[]float64{0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 13, 30, 60}, "source"),
		Fenced:     registry.Counter("fenced_writes_total", "Writes rejected by fencing", "source"),
		CacheStale: registry.Gauge("sensor_cache_stale", "1 when reads are served from last-known-good cache", "source"),
		Retries:    registry.Counter("sensor_retries_total", "Sensor retries by outcome", "source", "outcome"),
	}
}

func (m *Metrics) SetBreaker(state BreakerState) {
	if m == nil {
		return
	}
	value := 0.0
	switch state {
	case HalfOpen:
		value = 1
	case Open:
		value = 2
	}
	m.BreakerState.WithLabelValues(Source).Set(value)
}

type ClientConfig struct {
	BaseURL              string
	Token                string
	Timeout              time.Duration
	MaxAttempts          int
	RetryBudget          float64
	RetryBudgetPerSecond float64
	SlowThreshold        time.Duration
}

type Client struct {
	cfg     ClientConfig
	http    *http.Client
	breaker *Breaker
	budget  *retryBudget
	metrics *Metrics
	logger  *obs.Logger
}

type retryBudget struct {
	mu     sync.Mutex
	tokens float64
	max    float64
	refill float64
	last   time.Time
}

func newRetryBudget(max, refill float64) *retryBudget {
	if max <= 0 {
		max = 10
	}
	return &retryBudget{tokens: max, max: max, refill: refill, last: time.Now()}
}

func (b *retryBudget) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.refill
	if b.tokens > b.max {
		b.tokens = b.max
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func NewClient(cfg ClientConfig, breaker *Breaker, metrics *Metrics, logger *obs.Logger) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 3 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RetryBudgetPerSecond <= 0 {
		cfg.RetryBudgetPerSecond = 0.2
	}
	return &Client{
		cfg: cfg, metrics: metrics, logger: logger, breaker: breaker,
		budget: newRetryBudget(cfg.RetryBudget, cfg.RetryBudgetPerSecond),
		http: &http.Client{Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
}

func (c *Client) Stations(ctx context.Context) ([]Station, error) {
	var response struct {
		Items []Station `json:"items"`
	}
	if err := c.get(ctx, "stations", "/v1/stations", &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func (c *Client) Observations(ctx context.Context, stationID, cursor string, limit int) (Page, error) {
	query := url.Values{}
	query.Set("station_id", stationID)
	if limit > 0 {
		query.Set("limit", fmt.Sprint(limit))
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	var page Page
	err := c.get(ctx, "observations", "/v1/observations?"+query.Encode(), &page)
	return page, err
}

func (c *Client) get(ctx context.Context, operation, path string, out any) error {
	for attempt := 1; ; attempt++ {
		if !c.breaker.Allow() {
			if c.metrics != nil {
				c.metrics.Requests.WithLabelValues(Source, operation, "circuit_open").Inc()
			}
			return ErrCircuitOpen
		}
		start := time.Now()
		err, status, retryable := c.once(ctx, path, out)
		latency := time.Since(start)
		_ = status
		if c.metrics != nil {
			c.metrics.Duration.WithLabelValues(Source, operation).Observe(latency.Seconds())
		}
		if latency > c.cfg.SlowThreshold && c.cfg.SlowThreshold > 0 {
			c.breaker.Slow()
		}
		if err == nil {
			c.breaker.Success()
			if c.metrics != nil {
				c.metrics.Requests.WithLabelValues(Source, operation, "success").Inc()
			}
			return nil
		}
		if errors.Is(err, ErrCursorInvalid) {
			// A cursor reset after the sensor lost its emptyDir is an expected
			// state-reset event, not evidence that the sensor is unhealthy.
			c.breaker.Success()
			if c.metrics != nil {
				c.metrics.Requests.WithLabelValues(Source, operation, "cursor_invalid").Inc()
			}
			return err
		}
		c.breaker.Failure(fmt.Sprintf("%s: %v", operation, err))
		outcome := "retryable"
		switch {
		case errors.Is(err, ErrPermanent):
			outcome = "permanent"
		case errors.Is(err, ErrCursorInvalid):
			outcome = "cursor_invalid"
		}
		if c.metrics != nil {
			c.metrics.Requests.WithLabelValues(Source, operation, outcome).Inc()
		}
		if !retryable || attempt >= c.cfg.MaxAttempts || !c.budget.allow() {
			return err
		}
		if c.metrics != nil {
			c.metrics.Retries.WithLabelValues(Source, "attempted").Inc()
		}
		sleep := backoff(attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
}

func (c *Client) once(ctx context.Context, path string, out any) (error, int, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.cfg.BaseURL, "/")+path, nil)
	if err != nil {
		return err, 0, false
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("Accept", "application/json")
	obs.InjectHeaders(ctx, req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("sensor request: %w", err), 0, true
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("read sensor response: %w", err), resp.StatusCode, true
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out != nil {
			if err := json.Unmarshal(raw, out); err != nil {
				return fmt.Errorf("decode sensor response: %w", err), resp.StatusCode, false
			}
		}
		return nil, resp.StatusCode, false
	}
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusConflict, http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: HTTP %d %s", ErrCursorInvalid, resp.StatusCode, strings.TrimSpace(string(raw))), resp.StatusCode, false
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return fmt.Errorf("%w: HTTP %d %s", ErrPermanent, resp.StatusCode, strings.TrimSpace(string(raw))), resp.StatusCode, false
	case http.StatusTooManyRequests:
		return fmt.Errorf("sensor rate limited: HTTP 429"), resp.StatusCode, true
	default:
		return fmt.Errorf("sensor HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw))), resp.StatusCode, resp.StatusCode >= 500
	}
}

// Full jitter: sleep in [0, base*2^(attempt-1)], capped.
func backoff(attempt int) time.Duration {
	base := 200 * time.Millisecond
	ceiling := base << (attempt - 1)
	if ceiling > 3*time.Second {
		ceiling = 3 * time.Second
	}
	return time.Duration(rand.Int63n(int64(ceiling) + 1))
}
