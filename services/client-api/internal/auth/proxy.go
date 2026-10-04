package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
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
func NewAuthProxy(authURL string) *AuthProxy {
	return &AuthProxy{
		authURL: authURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Login forwards username/password to the auth service.
func (p *AuthProxy) Login(correlationID, username, password string) (map[string]any, error) {
	payload := map[string]string{
		"username": username,
		"password": password,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, p.authURL+"/auth/login", bytes.NewBuffer(body))
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
	payload := map[string]string{
		"refresh_token": refreshToken,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, p.authURL+"/auth/refresh", bytes.NewBuffer(body))
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
