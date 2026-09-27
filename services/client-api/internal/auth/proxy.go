package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// CasdoorProxy handles authentication by forwarding requests to Casdoor.
type CasdoorProxy struct {
	casdoorURL    string
	clientID      string
	clientSecret  string
	http          *http.Client
}

// NewCasdoorProxy creates a new Casdoor proxy.
func NewCasdoorProxy(casdoorURL, clientID, clientSecret string) *CasdoorProxy {
	return &CasdoorProxy{
		casdoorURL:   casdoorURL,
		clientID:     clientID,
		clientSecret: clientSecret,
		http:         &http.Client{Timeout: 10 * time.Second},
	}
}

// Login forwards username/password to Casdoor and returns the token response.
func (p *CasdoorProxy) Login(username, password string) (map[string]any, error) {
	payload := map[string]string{
		"grant_type": "password",
		"client_id":  p.clientID,
		"client_secret": p.clientSecret,
		"username":   username,
		"password":   password,
	}
	body, _ := json.Marshal(payload)

	resp, err := p.http.Post(p.casdoorURL+"/api/login/oauth/access_token", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("casdoor login: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("casdoor login: status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode casdoor response: %w", err)
	}
	return result, nil
}

// Refresh forwards the refresh token to Casdoor and returns the token response.
func (p *CasdoorProxy) Refresh(refreshToken string) (map[string]any, error) {
	payload := map[string]string{
		"grant_type":    "refresh_token",
		"client_id":     p.clientID,
		"client_secret": p.clientSecret,
		"refresh_token": refreshToken,
	}
	body, _ := json.Marshal(payload)

	resp, err := p.http.Post(p.casdoorURL+"/api/login/oauth/refresh_token", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("casdoor refresh: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("casdoor refresh: status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode casdoor response: %w", err)
	}
	return result, nil
}
