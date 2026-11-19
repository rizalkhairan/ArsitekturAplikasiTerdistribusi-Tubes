// Package storeclient talks to the canonical store service.
package storeclient

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

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
	"github.com/rizalkhairan/distributed-system/observability"
)

var (
	ErrFenced       = errors.New("storage rejected a stale authority epoch")
	ErrStaleVersion = errors.New("storage rejected a stale version")
	ErrNotFound     = errors.New("stored event not found")
)

type Client struct {
	base     string
	token    string
	instance string
	http     *http.Client
	audit    *http.Client
}

func New(baseURL, token, instance string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	transport := &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Client{
		base: strings.TrimRight(baseURL, "/"), token: token, instance: instance,
		http: &http.Client{Timeout: timeout, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
}

// SetAuditTimeout gives the audit reads their own budget. They traverse the
// whole table, so the upstream request timeout that suits a hazard read is too
// short for a checksum.
func (c *Client) SetAuditTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	c.audit = &http.Client{Timeout: timeout, Transport: c.http.Transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

func (c *Client) auditClient() *http.Client {
	if c.audit != nil {
		return c.audit
	}
	return c.http
}

// ErrVersionConflict means another payload already occupies this version.
var ErrVersionConflict = errors.New("canonical version conflict")

func (c *Client) Upsert(ctx context.Context, event database.HazardEvent, version, epoch int64, scope string) (created, ignored bool, err error) {
	return c.upsert(ctx, event, version, epoch, scope, false)
}

// UpsertAuto lets storage choose the next version under its row lock. A
// retried batch whose payload changed then resolves as an update instead of
// livelocking on version_conflict.
func (c *Client) UpsertAuto(ctx context.Context, event database.HazardEvent, version, epoch int64, scope string) (created, ignored bool, err error) {
	return c.upsert(ctx, event, version, epoch, scope, true)
}

func (c *Client) upsert(ctx context.Context, event database.HazardEvent, version, epoch int64, scope string, auto bool) (created, ignored bool, err error) {
	eventBody := map[string]any{
		"hazard_id": event.HazardID, "hazard_type": event.HazardType,
		"severity": event.Severity, "area_name": event.AreaName,
		"latitude": event.Latitude, "longitude": event.Longitude,
		"occurred_at": event.OccurredAt.UTC().Format(time.RFC3339Nano),
		"attributes":  event.Attributes,
	}
	if !event.IngestedAt.IsZero() {
		eventBody["ingested_at"] = event.IngestedAt.UTC().Format(time.RFC3339Nano)
	}
	body := map[string]any{
		"version": version, "operation": "upsert",
		"authority":      map[string]any{"partition": scope, "epoch": epoch},
		"correlation_id": obs.CorrelationID(ctx),
		"event":          eventBody,
	}
	if auto {
		body["auto_version"] = true
	}
	path := "/internal/canonical/events/" + url.PathEscape(event.Source) + "/" + url.PathEscape(event.SourceRefID)
	var response struct {
		HazardID string `json:"hazard_id"`
		Version  int64  `json:"version"`
		Replayed bool   `json:"replayed"`
	}
	status, err := c.doStatus(ctx, http.MethodPut, path, body, &response)
	if err != nil {
		var apiErr *Error
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
			if apiErr.Code == "stale_version" {
				return false, false, ErrStaleVersion
			}
			if apiErr.Code == "version_conflict" {
				return false, false, ErrVersionConflict
			}
			return false, false, ErrFenced
		}
		return false, false, err
	}
	return status == http.StatusCreated && !response.Replayed, response.Replayed, nil
}

// UpsertAdmin is the authenticated replay path (no poller authority claim).
func (c *Client) UpsertAdmin(ctx context.Context, event database.HazardEvent, version int64) error {
	eventBody := map[string]any{
		"hazard_id": event.HazardID, "hazard_type": event.HazardType,
		"severity": event.Severity, "area_name": event.AreaName,
		"latitude": event.Latitude, "longitude": event.Longitude,
		"occurred_at": event.OccurredAt.UTC().Format(time.RFC3339Nano),
		"attributes":  event.Attributes,
	}
	body := map[string]any{"version": version, "operation": "upsert", "event": eventBody}
	path := "/internal/admin/events/" + url.PathEscape(event.Source) + "/" + url.PathEscape(event.SourceRefID)
	_, err := c.doStatus(ctx, http.MethodPut, path, body, nil)
	return err
}

func (c *Client) Retract(ctx context.Context, source, ref string, version, epoch int64, scope string) error {
	body := map[string]any{
		"version": version, "operation": "retract",
		"authority":      map[string]any{"partition": scope, "epoch": epoch},
		"correlation_id": obs.CorrelationID(ctx),
	}
	path := "/internal/canonical/events/" + url.PathEscape(source) + "/" + url.PathEscape(ref)
	_, err := c.doStatus(ctx, http.MethodPut, path, body, nil)
	return err
}

// List uses the legacy occurred_at-ordered read helper for the client API.
func (c *Client) List(ctx context.Context, source, since string, limit int) ([]database.HazardEvent, error) {
	query := url.Values{}
	if source != "" {
		query.Set("source", source)
	}
	if since != "" {
		query.Set("since", since)
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprint(limit))
	}
	var events []database.HazardEvent
	err := c.do(ctx, http.MethodGet, "/events?"+query.Encode(), nil, &events)
	return events, err
}

// AuditEvents uses the contract query (ascending ingested_at, cursor).
func (c *Client) AuditEvents(ctx context.Context, source, since string, limit int, cursor string) (map[string]any, error) {
	query := url.Values{}
	if source != "" {
		query.Set("source", source)
	}
	if since != "" {
		query.Set("since", since)
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprint(limit))
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	var response map[string]any
	_, err := c.doStatusWith(ctx, c.auditClient(), http.MethodGet, "/internal/audit/events?"+query.Encode(), nil, &response)
	return response, err
}

func (c *Client) Get(ctx context.Context, sourceRefID string) (database.HazardEvent, error) {
	var event database.HazardEvent
	err := c.do(ctx, http.MethodGet, "/events/"+url.PathEscape(sourceRefID), nil, &event)
	if err != nil {
		var apiErr *Error
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
			return database.HazardEvent{}, ErrNotFound
		}
	}
	return event, err
}

func (c *Client) GetByHazardID(ctx context.Context, hazardID string) (database.HazardEvent, error) {
	var event database.HazardEvent
	err := c.do(ctx, http.MethodGet, "/events/hazard/"+url.PathEscape(hazardID), nil, &event)
	if err != nil {
		var apiErr *Error
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
			return database.HazardEvent{}, ErrNotFound
		}
	}
	return event, err
}

func (c *Client) Checksum(ctx context.Context, source string, partial bool) (map[string]any, error) {
	query := url.Values{}
	if source != "" {
		query.Set("source", source)
	}
	if partial {
		query.Set("partial", "true")
	}
	var response map[string]any
	_, err := c.doStatusWith(ctx, c.auditClient(), http.MethodGet, "/internal/audit/checksum?"+query.Encode(), nil, &response)
	return response, err
}

func (c *Client) Location(ctx context.Context, hazardID string) (map[string]any, error) {
	query := url.Values{}
	query.Set("hazard_id", hazardID)
	var response map[string]any
	_, err := c.doStatusWith(ctx, c.auditClient(), http.MethodGet, "/internal/audit/location?"+query.Encode(), nil, &response)
	return response, err
}

func (c *Client) Ready(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/readyz", nil, nil)
}

type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("storage %d %s: %s", e.Status, e.Code, e.Message)
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	_, err := c.doStatus(ctx, method, path, body, out)
	return err
}

func (c *Client) doStatus(ctx context.Context, method, path string, body, out any) (int, error) {
	return c.doStatusWith(ctx, c.http, method, path, body, out)
}

func (c *Client) doStatusWith(ctx context.Context, client *http.Client, method, path string, body, out any) (int, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("X-Caller-Instance", c.instance)
	obs.InjectHeaders(ctx, req.Header)
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("storage request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &apiErr)
		return resp.StatusCode, &Error{Status: resp.StatusCode, Code: apiErr.Error.Code, Message: apiErr.Error.Message}
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode storage response: %w", err)
		}
	}
	return resp.StatusCode, nil
}
