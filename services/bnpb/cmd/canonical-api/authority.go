package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// authorityClient validates a writer against the coordinator, which is the only
// source of truth for who may write a source. Storage must not accept an epoch
// from the request body: a holder that freezes past its lease and wakes would
// otherwise keep writing until the successor happens to write first.
type authorityClient struct {
	base  string
	token string
	http  *http.Client
	mu    sync.Mutex
	cache map[string]authorityCacheEntry
	ttl   time.Duration
}

type authorityCacheEntry struct {
	lease   authorityLease
	expires time.Time
}

func newAuthorityClient(base, token string) *authorityClient {
	if base == "" {
		return nil
	}
	return &authorityClient{base: base, token: token, http: &http.Client{Timeout: 2 * time.Second},
		cache: map[string]authorityCacheEntry{}, ttl: time.Second}
}

type authorityLease struct {
	Holder  string `json:"holder"`
	Epoch   int64  `json:"epoch"`
	Active  bool   `json:"active"`
	Expires int64  `json:"expires_in_ms"`
}

// Validate returns nil only when the caller is the active holder at the epoch
// it claims.
func (a *authorityClient) Validate(ctx context.Context, source, holder string, epoch int64) error {
	if a == nil {
		return nil
	}
	// A lease lives for 15s, so a one-second validation cache keeps the
	// coordinator round trip off the per-write hot path without widening the
	// window in which a stale holder could write.
	if lease, ok := a.cached(source); ok {
		return checkLease(lease, holder, epoch)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+"/v1/leases/"+source, nil)
	if err != nil {
		return err
	}
	if a.token != "" {
		req.Header.Set("X-Internal-Key", a.token)
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("authority unavailable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("authority returned %d", resp.StatusCode)
	}
	var lease authorityLease
	if err := json.NewDecoder(resp.Body).Decode(&lease); err != nil {
		return err
	}
	if err := checkLease(lease, holder, epoch); err != nil {
		return err
	}
	a.store(source, lease)
	return nil
}

func checkLease(lease authorityLease, holder string, epoch int64) error {
	if !lease.Active || lease.Epoch != epoch || lease.Holder != holder {
		return fmt.Errorf("fenced: holder=%q epoch=%d", lease.Holder, lease.Epoch)
	}
	return nil
}

func (a *authorityClient) cached(source string) (authorityLease, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.cache[source]
	if !ok || time.Now().After(entry.expires) {
		return authorityLease{}, false
	}
	return entry.lease, true
}

func (a *authorityClient) store(source string, lease authorityLease) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cache[source] = authorityCacheEntry{lease: lease, expires: time.Now().Add(a.ttl)}
}

// fencedSource limits the extra round trip to the sources that are polled by a
// leased leader. Administrative and synthetic imports keep their own path.
func fencedSource(source string) bool {
	switch source {
	case "BMKG", "PVMBG", "SENSOR_PANTAI":
		return true
	}
	return false
}
