package bnpb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/rizalkhairan/distributed-system/client-api/internal/middleware"
)

type Client struct {
	baseURL    string
	http       *http.Client
	internalKey string
}

func NewClient(baseURL string, timeout time.Duration, internalKey string) *Client {
	return &Client{
		baseURL:    baseURL,
		http:       &http.Client{Timeout: timeout},
		internalKey: internalKey,
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
	resp, err := c.http.Do(req)
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
	resp, err := c.http.Do(req)
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
