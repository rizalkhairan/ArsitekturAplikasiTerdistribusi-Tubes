package main

import (
	"context"
	"sync"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/canonical"
)

// listCache protects the legacy /events read path from repeated Citus
// traversals. Aggregator pods deliberately keep their own short cache, but
// rolling restarts and cache expiry must not fan out into a distributed query
// per caller. Expired entries are served while one background refresh runs.
type listCache struct {
	mu         sync.Mutex
	ttl        time.Duration
	entries    map[string]listEntry
	fills      map[string]*listFill
	refreshing map[string]bool
}

type listEntry struct {
	events  []canonical.Event
	expires time.Time
}

type listFill struct {
	done   chan struct{}
	events []canonical.Event
	err    error
}

func newListCache(ttl time.Duration) *listCache {
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	return &listCache{
		ttl:        ttl,
		entries:    map[string]listEntry{},
		fills:      map[string]*listFill{},
		refreshing: map[string]bool{},
	}
}

func (c *listCache) get(ctx context.Context, key string, load func(context.Context) ([]canonical.Event, error)) ([]canonical.Event, error) {
	now := time.Now()
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok {
		if now.Before(entry.expires) {
			events := entry.events
			c.mu.Unlock()
			return events, nil
		}
		if !c.refreshing[key] {
			c.refreshing[key] = true
			c.mu.Unlock()
			go c.refresh(key, load)
			return entry.events, nil
		}
		c.mu.Unlock()
		return entry.events, nil
	}
	if fill, ok := c.fills[key]; ok {
		c.mu.Unlock()
		select {
		case <-fill.done:
			return fill.events, fill.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	fill := &listFill{done: make(chan struct{})}
	c.fills[key] = fill
	c.mu.Unlock()

	events, err := load(ctx)
	c.mu.Lock()
	fill.events, fill.err = events, err
	delete(c.fills, key)
	if err == nil {
		c.entries[key] = listEntry{events: events, expires: time.Now().Add(c.ttl)}
	}
	close(fill.done)
	c.mu.Unlock()
	return events, err
}

func (c *listCache) refresh(key string, load func(context.Context) ([]canonical.Event, error)) {
	defer func() {
		c.mu.Lock()
		delete(c.refreshing, key)
		c.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	events, err := load(ctx)
	if err != nil {
		return
	}
	c.mu.Lock()
	c.entries[key] = listEntry{events: events, expires: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}

func (c *listCache) clear() {
	c.mu.Lock()
	c.entries = map[string]listEntry{}
	c.mu.Unlock()
}
