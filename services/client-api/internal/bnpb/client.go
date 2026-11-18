package bnpb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/rizalkhairan/distributed-system/client-api/internal/middleware"
)

const defaultRetryBudget = 8 * time.Second

type Client struct {
	baseURL     string
	http        *http.Client
	internalKey string
	retries     int
}

func NewClient(baseURL string, timeout time.Duration, internalKey string, retries int) *Client {
	if retries < 0 {
		retries = 0
	}
	// The default transport keeps only two idle connections per host. Under
	// load that turns every small burst into new TCP connections, which is
	// especially costly while Aggregator endpoints are churning. Keep enough
	// idle capacity for the admission limit.
	transport := &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Client{
		baseURL:     baseURL,
		http:        &http.Client{Timeout: timeout, Transport: transport},
		internalKey: internalKey,
		retries:     retries,
	}
}

// Hazard represents the canonical hazard event from BNPB Aggregator.
type Hazard struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  string         `json:"occurred_at"`
	IngestedAt  string         `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

var FreshnessHeaders = []string{
	"X-Data-Freshness",
	"X-Source-BMKG-Status",
	"X-Source-BMKG-Last-Success",
	"X-Source-BMKG-Stale-Since",
	"X-Source-BMKG-Error",
	"X-Source-PVMBG-Status",
	"X-Source-PVMBG-Last-Success",
	"X-Source-PVMBG-Stale-Since",
	"X-Source-PVMBG-Error",
}

func CopyFreshnessHeaders(dst, src http.Header) {
	for _, name := range FreshnessHeaders {
		if value := src.Get(name); value != "" {
			dst.Set(name, value)
		}
	}
}

func (c *Client) ListHazards(ctx context.Context, source, severity, since string, limit int) ([]Hazard, http.Header, error) {
	u, _ := url.Parse(c.baseURL + "/hazards")
	q := u.Query()
	if source != "" {
		q.Set("source", source)
	}
	if severity != "" {
		q.Set("severity", severity)
	}
	if since != "" {
		q.Set("since", since)
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, err
	}

	// Propagate correlation ID
	corrID := middleware.GetCorrelationID(ctx)
	if corrID != "" {
		req.Header.Set("X-Correlation-ID", corrID)
	}

	// Send internal shared secret
	if c.internalKey != "" {
		req.Header.Set("X-Internal-Key", c.internalKey)
	}

	start := time.Now()
	resp, err := c.doWithRetry(ctx, req)
	latency := time.Since(start)
	if err != nil {
		log.Printf(`{"corr_id":"%s","method":"GET","path":%q,"latency_ms":%d,"error":"%s"}`, corrID, u.String(), latency.Milliseconds(), err.Error())
		return nil, nil, fmt.Errorf("bnpb list: %w", err)
	}
	defer resp.Body.Close()

	log.Printf(`{"corr_id":"%s","method":"GET","path":%q,"status":%d,"latency_ms":%d}`, corrID, u.String(), resp.StatusCode, latency.Milliseconds())

	if resp.StatusCode != http.StatusOK {
		return nil, resp.Header, fmt.Errorf("bnpb list: status %d", resp.StatusCode)
	}

	var hazards []Hazard
	if err := json.NewDecoder(resp.Body).Decode(&hazards); err != nil {
		return nil, resp.Header, fmt.Errorf("bnpb list decode: %w", err)
	}
	return hazards, resp.Header, nil
}

func (c *Client) GetHazard(ctx context.Context, id string) (*Hazard, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/hazards/"+id, nil)
	if err != nil {
		return nil, nil, err
	}

	corrID := middleware.GetCorrelationID(ctx)
	if corrID != "" {
		req.Header.Set("X-Correlation-ID", corrID)
	}

	if c.internalKey != "" {
		req.Header.Set("X-Internal-Key", c.internalKey)
	}

	start := time.Now()
	resp, err := c.doWithRetry(ctx, req)
	latency := time.Since(start)
	if err != nil {
		log.Printf(`{"corr_id":"%s","method":"GET","path":%q,"latency_ms":%d,"error":"%s"}`, corrID, "/hazards/"+id, latency.Milliseconds(), err.Error())
		return nil, nil, fmt.Errorf("bnpb get: %w", err)
	}
	defer resp.Body.Close()

	log.Printf(`{"corr_id":"%s","method":"GET","path":%q,"status":%d,"latency_ms":%d}`, corrID, "/hazards/"+id, resp.StatusCode, latency.Milliseconds())

	if resp.StatusCode == http.StatusNotFound {
		return nil, resp.Header, fmt.Errorf("hazard not found")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp.Header, fmt.Errorf("bnpb get: status %d", resp.StatusCode)
	}

	var hazard Hazard
	if err := json.NewDecoder(resp.Body).Decode(&hazard); err != nil {
		return nil, resp.Header, fmt.Errorf("bnpb get decode: %w", err)
	}
	return &hazard, resp.Header, nil
}

// doWithRetry retries only idempotent read requests. It retries transport
// failures and gateway/unavailable responses, which are the errors produced
// while an Aggregator endpoint is being added or removed. Client errors such as
// 404 are returned immediately.
func (c *Client) doWithRetry(ctx context.Context, req *http.Request) (*http.Response, error) {
	retryCtx, cancel := context.WithTimeout(ctx, defaultRetryBudget)
	delivered := false
	defer func() {
		if !delivered {
			cancel()
		}
	}()

	attempts := c.retries + 1
	var lastResp *http.Response
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			delay := retryDelay(attempt)
			timer := time.NewTimer(delay)
			select {
			case <-retryCtx.Done():
				timer.Stop()
				return lastResp, retryCtx.Err()
			case <-timer.C:
			}
		}

		resp, err := c.http.Do(req.Clone(retryCtx))
		if err == nil && !retryableStatus(resp.StatusCode) {
			// Collect the body inside the retry boundary. A proxy endpoint
			// that sends headers and then stalls must be retried rather than
			// surfacing as a decode timeout after the retry loop has ended.
			raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
			_ = resp.Body.Close()
			if readErr != nil {
				lastErr = fmt.Errorf("read body: %w", readErr)
				if retryCtx.Err() != nil {
					return nil, retryCtx.Err()
				}
				continue
			}
			resp.Body = io.NopCloser(bytes.NewReader(raw))
			delivered = true
			resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
			return resp, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			// Drain a bounded amount so the connection can be reused; the
			// response body is not useful after a retryable status.
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			lastResp = resp
		}
		if retryCtx.Err() != nil {
			return lastResp, retryCtx.Err()
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("upstream request failed")
	}
	return lastResp, lastErr
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func retryDelay(attempt int) time.Duration {
	// 50ms, 100ms, 200ms, 400ms, then 800ms capped. With the default eight
	// retries this covers the window in which Kubernetes removes a terminating
	// Aggregator endpoint, while staying below the HTTP write timeout.
	base := 50 * time.Millisecond << (attempt - 2)
	if base > 800*time.Millisecond {
		base = 800 * time.Millisecond
	}
	jitter := time.Duration(rand.Int63n(int64(base/2) + 1))
	return base + jitter
}
