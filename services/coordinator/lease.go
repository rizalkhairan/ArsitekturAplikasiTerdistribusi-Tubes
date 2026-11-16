package main

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrLeaseHeld    = errors.New("lease held by another holder")
	ErrStaleEpoch   = errors.New("stale epoch")
	ErrLeaseExpired = errors.New("lease expired")
)

// Clock separates monotonic lease expiry from wall-clock display.
type Clock interface {
	Mono() time.Time
	Wall() time.Time
}

type wallClock struct{}

func (wallClock) Mono() time.Time { return time.Now() }
func (wallClock) Wall() time.Time { return time.Now().UTC() }

type leaseState struct {
	Source      string
	Holder      string
	Epoch       int64
	ExpiresMono time.Time
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
	Status       string         `json:"status"` // pending|delivering|delivered|failed
	Attempts     int            `json:"attempts"`
	ClaimedBy    string         `json:"claimed_by,omitempty"`
	ClaimedUntil string         `json:"claimed_until,omitempty"`
	CreatedAt    string         `json:"created_at"`
	DeliveredAt  string         `json:"delivered_at,omitempty"`
	LastError    string         `json:"last_error,omitempty"`

	claimMono time.Time
}

type persisted struct {
	Epochs      map[string]int64        `json:"epochs"`
	Checkpoints map[string]Checkpoint   `json:"checkpoints"`
	Outbox      map[string]*OutboxEntry `json:"outbox"`
	Sequence    int64                   `json:"sequence"`
}

type Manager struct {
	mu          sync.Mutex
	clock       Clock
	epochs      map[string]int64
	leases      map[string]*leaseState
	checkpoints map[string]Checkpoint
	outbox      map[string]*OutboxEntry
	dedupe      map[string]string // dedupe key -> outbox id
	sequence    int64
	persist     func(persisted) error

	OnLeaseTransition func(source, holder, event string, epoch int64)
}

func NewManager(clock Clock, persist func(persisted) error) *Manager {
	return &Manager{
		clock: clock, epochs: map[string]int64{}, leases: map[string]*leaseState{},
		checkpoints: map[string]Checkpoint{}, outbox: map[string]*OutboxEntry{},
		dedupe: map[string]string{}, persist: persist,
	}
}

func (m *Manager) snapshotLocked() persisted {
	// Active lease deadlines are not copied; a restart invalidates every lease.
	epochs := make(map[string]int64, len(m.epochs))
	for k, v := range m.epochs {
		epochs[k] = v
	}
	checkpoints := make(map[string]Checkpoint, len(m.checkpoints))
	for k, v := range m.checkpoints {
		checkpoints[k] = v
	}
	outbox := make(map[string]*OutboxEntry, len(m.outbox))
	for k, v := range m.outbox {
		copy := *v
		if copy.Status == "delivering" {
			copy.Status = "pending"
			copy.ClaimedBy = ""
			copy.ClaimedUntil = ""
		}
		outbox[k] = &copy
	}
	return persisted{Epochs: epochs, Checkpoints: checkpoints, Outbox: outbox, Sequence: m.sequence}
}

func (m *Manager) saveLocked() error {
	if m.persist == nil {
		return nil
	}
	return m.persist(m.snapshotLocked())
}

// Restore loads durable state; active leases are never restored.
func (m *Manager) Restore(state persisted) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state.Epochs != nil {
		m.epochs = state.Epochs
	}
	if state.Checkpoints != nil {
		m.checkpoints = state.Checkpoints
	}
	if state.Outbox != nil {
		for id, entry := range state.Outbox {
			if entry.Status == "delivering" {
				entry.Status = "pending"
				entry.ClaimedBy = ""
				entry.ClaimedUntil = ""
			}
			m.outbox[id] = entry
			if entry.DedupeKey != "" {
				m.dedupe[entry.DedupeKey] = id
			}
		}
	}
	m.sequence = state.Sequence
}

func (m *Manager) Acquire(source, holder string, ttl time.Duration) (leaseState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Mono()
	current := m.leases[source]
	if current != nil && now.Before(current.ExpiresMono) {
		if current.Holder != holder {
			return *current, ErrLeaseHeld
		}
		// Same holder re-acquires after a failed renew: keep the epoch.
		current.ExpiresMono = now.Add(ttl)
		if err := m.saveLocked(); err != nil {
			return leaseState{}, err
		}
		return *current, nil
	}
	m.epochs[source]++
	if current != nil && current.Holder != holder {
		if m.OnLeaseTransition != nil {
			m.OnLeaseTransition(source, current.Holder, "expired", current.Epoch)
		}
	}
	lease := &leaseState{Source: source, Holder: holder, Epoch: m.epochs[source], ExpiresMono: now.Add(ttl)}
	m.leases[source] = lease
	if err := m.saveLocked(); err != nil {
		return leaseState{}, err
	}
	if m.OnLeaseTransition != nil {
		m.OnLeaseTransition(source, holder, "acquired", lease.Epoch)
	}
	return *lease, nil
}

func (m *Manager) Renew(source, holder string, epoch int64, ttl time.Duration) (leaseState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Mono()
	current := m.leases[source]
	if current == nil || current.Holder != holder || current.Epoch != epoch {
		return leaseState{}, ErrStaleEpoch
	}
	if !now.Before(current.ExpiresMono) {
		delete(m.leases, source)
		if m.OnLeaseTransition != nil {
			m.OnLeaseTransition(source, holder, "expired", current.Epoch)
		}
		return leaseState{}, ErrLeaseExpired
	}
	current.ExpiresMono = now.Add(ttl)
	if err := m.saveLocked(); err != nil {
		return leaseState{}, err
	}
	return *current, nil
}

func (m *Manager) Release(source, holder string, epoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.leases[source]
	if current != nil && current.Holder == holder && current.Epoch == epoch {
		delete(m.leases, source)
		if m.OnLeaseTransition != nil {
			m.OnLeaseTransition(source, holder, "released", epoch)
		}
	}
	return m.saveLocked()
}

func (m *Manager) Lease(source string) (leaseState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.leases[source]
	if current == nil {
		return leaseState{Source: source, Epoch: m.epochs[source]}, false
	}
	active := m.clock.Mono().Before(current.ExpiresMono)
	if !active {
		delete(m.leases, source)
		if m.OnLeaseTransition != nil {
			m.OnLeaseTransition(source, current.Holder, "expired", current.Epoch)
		}
	}
	return *current, active
}

// CommitCheckpoint rejects stale or expired holders.
func (m *Manager) CommitCheckpoint(source string, epoch int64, cursor string) (Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.leases[source]
	now := m.clock.Mono()
	if current == nil || current.Epoch != epoch || !now.Before(current.ExpiresMono) {
		return Checkpoint{}, ErrStaleEpoch
	}
	checkpoint := Checkpoint{Source: source, Cursor: cursor, Epoch: epoch,
		UpdatedAt: m.clock.Wall().Format(time.RFC3339Nano)}
	m.checkpoints[source] = checkpoint
	if err := m.saveLocked(); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}

func (m *Manager) Checkpoint(source string) Checkpoint {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cp, ok := m.checkpoints[source]; ok {
		return cp
	}
	return Checkpoint{Source: source}
}

func (m *Manager) Enqueue(dedupeKey string, payload map[string]any) (OutboxEntry, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.dedupe[dedupeKey]; ok {
		return *m.outbox[id], false, nil
	}
	m.sequence++
	id := fmt.Sprintf("ob-%012d", m.sequence)
	entry := &OutboxEntry{ID: id, DedupeKey: dedupeKey, Payload: payload, Status: "pending",
		CreatedAt: m.clock.Wall().Format(time.RFC3339Nano)}
	m.outbox[id] = entry
	m.dedupe[dedupeKey] = id
	if err := m.saveLocked(); err != nil {
		return OutboxEntry{}, false, err
	}
	return *entry, true, nil
}

func (m *Manager) Claim(holder string, limit int, lease time.Duration) []OutboxEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Mono()
	ids := make([]string, 0, len(m.outbox))
	for id, entry := range m.outbox {
		delayPassed := entry.claimMono.IsZero() || !now.Before(entry.claimMono)
		claimable := (entry.Status == "pending" && delayPassed) ||
			(entry.Status == "delivering" && !now.Before(entry.claimMono))
		if claimable {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]OutboxEntry, 0, len(ids))
	for _, id := range ids {
		entry := m.outbox[id]
		entry.Status = "delivering"
		entry.ClaimedBy = holder
		entry.ClaimedUntil = m.clock.Wall().Add(lease).Format(time.RFC3339Nano)
		entry.claimMono = now.Add(lease)
		entry.Attempts++
		out = append(out, *entry)
	}
	if len(out) > 0 {
		_ = m.saveLocked()
	}
	return out
}

func (m *Manager) Ack(id, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.outbox[id]
	if !ok {
		return errors.New("outbox entry not found")
	}
	entry.Status = "delivered"
	entry.DeliveredAt = m.clock.Wall().Format(time.RFC3339Nano)
	entry.ClaimedBy = ""
	entry.ClaimedUntil = ""
	entry.LastError = ""
	return m.saveLocked()
}

func (m *Manager) Nack(id, holder, reason string, retryAfter time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.outbox[id]
	if !ok {
		return errors.New("outbox entry not found")
	}
	entry.Status = "pending"
	entry.ClaimedBy = ""
	entry.ClaimedUntil = ""
	entry.LastError = reason
	entry.claimMono = m.clock.Mono().Add(retryAfter) // not claimable until this passes? see Claim
	return m.saveLocked()
}

func (m *Manager) Outbox(status string, limit int) []OutboxEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]OutboxEntry, 0, len(m.outbox))
	for _, entry := range m.outbox {
		if status != "" && entry.Status != status {
			continue
		}
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (m *Manager) OutboxStats() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := map[string]int{"pending": 0, "delivering": 0, "delivered": 0, "failed": 0}
	for _, entry := range m.outbox {
		stats[entry.Status]++
	}
	return stats
}
