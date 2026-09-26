package communication

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type PVMBGClient struct {
	baseURL string
	token   string
	http    *http.Client
}

func newPVMBGClient(baseURL, token string, timeout time.Duration) *PVMBGClient {
	return &PVMBGClient{
		baseURL: strings.TrimRight(baseURL, "/"), token: token,
		http: &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
}

func (c *PVMBGClient) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("PVMBG request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("PVMBG returned HTTP %d", response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(out); err != nil {
		return fmt.Errorf("decode PVMBG response: %w", err)
	}
	return nil
}

func (c *PVMBGClient) Health(ctx context.Context) error {
	var response struct {
		Status string `json:"status"`
	}
	if err := c.get(ctx, "/health", &response); err != nil {
		return err
	}
	if response.Status != "ok" {
		return fmt.Errorf("PVMBG is not healthy")
	}
	return nil
}

// VolcanicReports retains source fields for the future canonical mapping layer.
func (c *PVMBGClient) VolcanicReports(ctx context.Context, since string) ([]json.RawMessage, error) {
	var response struct {
		Reports []json.RawMessage `json:"reports"`
	}
	err := c.get(ctx, "/volcanic-reports?"+url.Values{"since": {since}}.Encode(), &response)
	return response.Reports, err
}
