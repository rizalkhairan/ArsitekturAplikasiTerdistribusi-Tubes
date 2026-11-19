// Package dlqclient talks to C's ingest-pipeline DLQ contract.
package dlqclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rizalkhairan/distributed-system/observability"
)

type Client struct {
	base       string
	token      string
	auditToken string
	http       *http.Client
}

func New(baseURL, token string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Client{base: strings.TrimRight(baseURL, "/"), token: token,
		http: &http.Client{Timeout: timeout}}
}

// Park durably hands a raw invalid payload to C's ingest pipeline.
func (c *Client) Park(ctx context.Context, source string, raw any, reason string, retryable bool) error {
	rawJSON, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	sourceRef := ""
	if object, ok := raw.(map[string]any); ok {
		if value, ok := object["observation_id"].(string); ok {
			sourceRef = value
		}
	}
	return c.ParkRaw(ctx, source, sourceRef, rawJSON, reason, retryable)
}

// ParkRaw parks exact original bytes without re-encoding them.
func (c *Client) ParkRaw(ctx context.Context, source, sourceRef string, raw []byte, reason string, retryable bool) error {
	h := sha256.New()
	h.Write([]byte(source))
	h.Write([]byte{0})
	h.Write([]byte(sourceRef))
	h.Write([]byte{0})
	h.Write(raw)
	body := map[string]any{
		"message_id":     hex.EncodeToString(h.Sum(nil)),
		"source":         source,
		"source_ref_id":  sourceRef,
		"reason":         reason,
		"retryable":      retryable,
		"correlation_id": obs.CorrelationID(ctx),
		"raw_base64":     base64.StdEncoding.EncodeToString(raw),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/internal/dlq/park", nil)
	if err != nil {
		return err
	}
	rawBody, _ := json.Marshal(body)
	req.Body = io.NopCloser(bytes.NewReader(rawBody))
	req.ContentLength = int64(len(rawBody))
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	obs.InjectHeaders(ctx, req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("dlq park: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		rawResp, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return fmt.Errorf("dlq park returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(rawResp)))
	}
	return nil
}

func (c *Client) SetAuditToken(token string) { c.auditToken = token }

// StatusError preserves the upstream HTTP status so the Aggregator can return
// the same code instead of turning every 4xx into a 503.
type StatusError struct {
	Status  int
	Message string
}

func (e *StatusError) Error() string { return fmt.Sprintf("dlq HTTP %d: %s", e.Status, e.Message) }

func (c *Client) Unprocessed(ctx context.Context, source, since string, limit int, partial bool, cursor string) (map[string]any, error) {
	token := c.auditToken
	if token == "" {
		token = c.token
	}
	query := url.Values{}
	if source != "" {
		query.Set("source", source)
	}
	if since != "" {
		query.Set("since", since)
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprint(limit))
	}
	var response map[string]any
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/internal/audit/unprocessed?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	obs.InjectHeaders(ctx, req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dlq unprocessed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(raw))
		var apiErr struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Error.Message != "" {
			message = apiErr.Error.Message
		}
		return nil, &StatusError{Status: resp.StatusCode, Message: message}
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ingest pipeline HTTP %d", resp.StatusCode)
	}
	return nil
}
