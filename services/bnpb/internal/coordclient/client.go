// Package coordclient is the Aggregator's client for the coordination store.
package coordclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rizalkhairan/distributed-system/observability"
)

var (
	ErrLeaseHeld  = errors.New("lease held by another holder")
	ErrStaleEpoch = errors.New("stale epoch")
)

type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("coordinator %d %s: %s", e.Status, e.Code, e.Message)
}

type Client struct {
	base  string
	token string
	http  *http.Client
}

func New(baseURL, token string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		base: strings.TrimRight(baseURL, "/"), token: token,
		http: &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
}

type Lease struct {
	Source      string `json:"source"`
	Holder      string `json:"holder"`
	Epoch       int64  `json:"epoch"`
	Active      bool   `json:"active"`
	ExpiresInMs int64  `json:"expires_in_ms"`
}

type Checkpoint struct {
	Source    string `json:"source"`
	Cursor    string `json:"cursor"`
	Epoch     int64  `json:"epoch"`
	UpdatedAt string `json:"updated_at"`
}

type OutboxEntry struct {
	ID           string         `json:"id"`
	DedupeKey    string         `json:"dedupe_key"`
	Payload      map[string]any `json:"payload"`
	Status       string         `json:"status"`
	Attempts     int            `json:"attempts"`
	ClaimedBy    string         `json:"claimed_by"`
	ClaimedUntil string         `json:"claimed_until"`
	CreatedAt    string         `json:"created_at"`
	DeliveredAt  string         `json:"delivered_at"`
	LastError    string         `json:"last_error"`
}

func (c *Client) Acquire(ctx context.Context, source, holder string, ttl time.Duration) (Lease, error) {
	var lease Lease
	err := c.do(ctx, http.MethodPost, "/v1/leases/"+url.PathEscape(source)+"/acquire",
		map[string]any{"holder": holder, "ttl_ms": ttl.Milliseconds()}, &lease)
	if err != nil {
		var apiErr *Error
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict && apiErr.Code == "lease_held" {
			return Lease{}, ErrLeaseHeld
		}
	}
	return lease, err
}

func (c *Client) Renew(ctx context.Context, source, holder string, epoch int64, ttl time.Duration) (Lease, error) {
	var lease Lease
	err := c.do(ctx, http.MethodPost, "/v1/leases/"+url.PathEscape(source)+"/renew",
		map[string]any{"holder": holder, "epoch": epoch, "ttl_ms": ttl.Milliseconds()}, &lease)
	if err != nil {
		var apiErr *Error
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
			return Lease{}, ErrStaleEpoch
		}
	}
	return lease, err
}

func (c *Client) Release(ctx context.Context, source, holder string, epoch int64) error {
	return c.do(ctx, http.MethodPost, "/v1/leases/"+url.PathEscape(source)+"/release",
		map[string]any{"holder": holder, "epoch": epoch}, nil)
}

func (c *Client) Checkpoint(ctx context.Context, source string) (Checkpoint, error) {
	var cp Checkpoint
	err := c.do(ctx, http.MethodGet, "/v1/checkpoints/"+url.PathEscape(source), nil, &cp)
	return cp, err
}

// Validate refreshes the holder's status with the store immediately before a
// write. A process that was suspended past its TTL must call this and see 409
// before it is allowed to persist anything.
func (c *Client) Validate(ctx context.Context, source, holder string, epoch int64) error {
	var lease Lease
	if err := c.do(ctx, http.MethodGet, "/v1/leases/"+url.PathEscape(source), nil, &lease); err != nil {
		return err
	}
	if !lease.Active || lease.Holder != holder || lease.Epoch != epoch {
		return ErrStaleEpoch
	}
	return nil
}

func (c *Client) CommitCheckpoint(ctx context.Context, source string, epoch int64, cursor string) (Checkpoint, error) {
	var cp Checkpoint
	err := c.do(ctx, http.MethodPut, "/v1/checkpoints/"+url.PathEscape(source),
		map[string]any{"epoch": epoch, "cursor": cursor}, &cp)
	if err != nil {
		var apiErr *Error
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
			return Checkpoint{}, ErrStaleEpoch
		}
	}
	return cp, err
}

func (c *Client) Enqueue(ctx context.Context, dedupeKey string, payload map[string]any) (OutboxEntry, bool, error) {
	var response struct {
		Entry   OutboxEntry `json:"entry"`
		Created bool        `json:"created"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/outbox",
		map[string]any{"dedupe_key": dedupeKey, "payload": payload}, &response)
	return response.Entry, response.Created, err
}

func (c *Client) Claim(ctx context.Context, holder string, limit int, lease time.Duration) ([]OutboxEntry, error) {
	var entries []OutboxEntry
	err := c.do(ctx, http.MethodPost, "/v1/outbox/claim",
		map[string]any{"holder": holder, "limit": limit, "lease_ms": lease.Milliseconds()}, &entries)
	return entries, err
}

func (c *Client) Ack(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/v1/outbox/"+url.PathEscape(id)+"/ack", nil, nil)
}

func (c *Client) Nack(ctx context.Context, id, reason string, retryAfter time.Duration) error {
	return c.do(ctx, http.MethodPost, "/v1/outbox/"+url.PathEscape(id)+"/nack",
		map[string]any{"reason": reason, "retry_after_ms": retryAfter.Milliseconds()}, nil)
}

func (c *Client) OutboxStats(ctx context.Context) (map[string]int, error) {
	var stats map[string]int
	err := c.do(ctx, http.MethodGet, "/v1/outbox/stats", nil, &stats)
	return stats, err
}

func (c *Client) Ready(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/readyz", nil, nil)
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("X-Internal-Key", c.token)
	}
	obs.InjectHeaders(ctx, req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("coordinator request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &apiErr)
		return &Error{Status: resp.StatusCode, Code: apiErr.Error.Code, Message: apiErr.Error.Message}
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decode coordinator response: %w", err)
		}
	}
	return nil
}
