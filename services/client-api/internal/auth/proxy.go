package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/rizalkhairan/distributed-system/shared/telemetry"
	"io"
	"net/http"
	"time"
)

// AuthProxy handles authentication by forwarding requests to the auth service.
type AuthProxy struct {
	authURL string
	http    *http.Client
}

// NewAuthProxy creates a new auth proxy.
func NewAuthProxy(authURL string, runtimes ...*telemetry.Runtime) *AuthProxy {
	proxy := &AuthProxy{
		authURL: authURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
	if len(runtimes) > 0 && runtimes[0] != nil {
		proxy.http.Transport = runtimes[0].Transport(nil)
	}
	return proxy
}

// Login forwards username/password to the auth service.
func (p *AuthProxy) Login(correlationID, username, password string) (map[string]any, error) {
	return p.LoginContext(telemetry.WithCorrelation(context.Background(), correlationID), username, password)
}

func (p *AuthProxy) LoginContext(ctx context.Context, username, password string) (map[string]any, error) {
	correlationID := telemetry.CorrelationID(ctx)
	payload := map[string]string{
		"username": username,
		"password": password,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.authURL+"/auth/login", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("auth login: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if correlationID != "" {
		req.Header.Set("X-Correlation-ID", correlationID)
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("auth login: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth login: status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode auth response: %w", err)
	}
	return result, nil
}

// Refresh forwards the refresh token to the auth service.
func (p *AuthProxy) Refresh(correlationID, refreshToken string) (map[string]any, error) {
	return p.RefreshContext(telemetry.WithCorrelation(context.Background(), correlationID), refreshToken)
}

func (p *AuthProxy) RefreshContext(ctx context.Context, refreshToken string) (map[string]any, error) {
	correlationID := telemetry.CorrelationID(ctx)
	payload := map[string]string{
		"refresh_token": refreshToken,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.authURL+"/auth/refresh", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("auth refresh: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if correlationID != "" {
		req.Header.Set("X-Correlation-ID", correlationID)
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("auth refresh: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth refresh: status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode auth response: %w", err)
	}
	return result, nil
}
