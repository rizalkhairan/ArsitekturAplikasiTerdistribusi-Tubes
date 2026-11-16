package main

import (
	"testing"
	"time"
)

type fakeClock struct {
	mono time.Time
	wall time.Time
}

func newFakeClock() *fakeClock {
	now := time.Now()
	return &fakeClock{mono: now, wall: now}
}

func (c *fakeClock) Mono() time.Time             { return c.mono }
func (c *fakeClock) Wall() time.Time             { return c.wall }
func (c *fakeClock) AdvanceMono(d time.Duration) { c.mono = c.mono.Add(d) }
func (c *fakeClock) ShiftWall(d time.Duration)   { c.wall = c.wall.Add(d) }

func TestAcquireIsSingletonAndIncrementsEpoch(t *testing.T) {
	clock := newFakeClock()
	m := NewManager(clock, nil)

	first, err := m.Acquire("BMKG", "pod-a", 15*time.Second)
	if err != nil || first.Epoch != 1 {
		t.Fatalf("first acquire = %+v, %v; want epoch 1", first, err)
	}
	if _, err := m.Acquire("BMKG", "pod-b", 15*time.Second); err != ErrLeaseHeld {
		t.Fatalf("second holder error = %v, want ErrLeaseHeld", err)
	}
	clock.AdvanceMono(16 * time.Second)
	second, err := m.Acquire("BMKG", "pod-b", 15*time.Second)
	if err != nil || second.Epoch != 2 {
		t.Fatalf("takeover = %+v, %v; want epoch 2", second, err)
	}
}

func TestWallClockShiftDoesNotExpireLease(t *testing.T) {
	clock := newFakeClock()
	m := NewManager(clock, nil)
	lease, err := m.Acquire("BMKG", "pod-a", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Node clock jumps +45s, then -30s. Monotonic time has only advanced 1s.
	clock.ShiftWall(45 * time.Second)
	clock.AdvanceMono(time.Second)
	if _, err := m.Renew("BMKG", "pod-a", lease.Epoch, 15*time.Second); err != nil {
		t.Fatalf("renew after +45s wall shift: %v", err)
	}
	clock.ShiftWall(-30 * time.Second)
	clock.AdvanceMono(time.Second)
	if _, err := m.Renew("BMKG", "pod-a", lease.Epoch, 15*time.Second); err != nil {
		t.Fatalf("renew after -30s wall shift: %v", err)
	}
	// Monotonic expiry still works.
	clock.AdvanceMono(20 * time.Second)
	if _, err := m.Renew("BMKG", "pod-a", lease.Epoch, 15*time.Second); err != ErrLeaseExpired {
		t.Fatalf("renew after monotonic expiry = %v, want ErrLeaseExpired", err)
	}
}

// A frozen holder (VM suspend / SIGSTOP) may not see monotonic time advance and
// may wake up believing its lease is valid. The store's clock does advance, so a
// new holder gets a higher epoch and the old holder's CAS is fenced.
func TestFrozenHolderCannotCommitAfterTakeover(t *testing.T) {
	clock := newFakeClock()
	m := NewManager(clock, nil)
	old, err := m.Acquire("BMKG", "frozen-pod", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// The store observes TTL expiry while the holder is frozen.
	clock.AdvanceMono(16 * time.Second)
	newLease, err := m.Acquire("BMKG", "awake-pod", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Epoch != old.Epoch+1 {
		t.Fatalf("new epoch = %d, want %d", newLease.Epoch, old.Epoch+1)
	}

	if _, err := m.CommitCheckpoint("BMKG", old.Epoch, "seismic=2026-01-01T00:00:01Z"); err != ErrStaleEpoch {
		t.Fatalf("frozen holder commit = %v, want ErrStaleEpoch", err)
	}
	if cp := m.Checkpoint("BMKG"); cp.Cursor != "" {
		t.Fatalf("checkpoint advanced by stale holder: %+v", cp)
	}
	if _, err := m.CommitCheckpoint("BMKG", newLease.Epoch, "seismic=2026-01-01T00:00:02Z"); err != nil {
		t.Fatalf("current holder commit: %v", err)
	}
}

func TestCheckpointCASRejectsSplitBrain(t *testing.T) {
	clock := newFakeClock()
	m := NewManager(clock, nil)
	a, _ := m.Acquire("PVMBG", "pod-a", 15*time.Second)
	clock.AdvanceMono(16 * time.Second)
	b, _ := m.Acquire("PVMBG", "pod-b", 15*time.Second)

	if _, err := m.CommitCheckpoint("PVMBG", a.Epoch, "a"); err != ErrStaleEpoch {
		t.Fatalf("stale CAS = %v, want ErrStaleEpoch", err)
	}
	if _, err := m.CommitCheckpoint("PVMBG", b.Epoch, "b"); err != nil {
		t.Fatalf("current CAS: %v", err)
	}
}

func TestRestartInvalidatesLeasesButKeepsEpoch(t *testing.T) {
	clock := newFakeClock()
	var saved persisted
	m := NewManager(clock, func(p persisted) error { saved = p; return nil })
	if _, err := m.Acquire("BMKG", "pod-a", 15*time.Second); err != nil {
		t.Fatal(err)
	}

	restarted := NewManager(clock, nil)
	restarted.Restore(saved)
	if _, active := restarted.Lease("BMKG"); active {
		t.Fatal("lease survived coordinator restart")
	}
	lease, err := restarted.Acquire("BMKG", "pod-b", 15*time.Second)
	if err != nil || lease.Epoch != 2 {
		t.Fatalf("post-restart acquire = %+v, %v; want epoch 2", lease, err)
	}
}

func TestOutboxDedupeAndAtLeastOnceClaim(t *testing.T) {
	clock := newFakeClock()
	m := NewManager(clock, nil)
	first, created, err := m.Enqueue("station|observed|obs", map[string]any{"event_id": "obs"})
	if err != nil || !created {
		t.Fatalf("enqueue: %+v %v %v", first, created, err)
	}
	second, created, err := m.Enqueue("station|observed|obs", map[string]any{"event_id": "obs"})
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("dedupe: %+v %v %v", second, created, err)
	}
	claims := m.Claim("pod-a", 10, 15*time.Second)
	if len(claims) != 1 {
		t.Fatalf("claims = %d, want 1", len(claims))
	}
	// Claim expires; another pod may redeliver (at-least-once).
	clock.AdvanceMono(16 * time.Second)
	claims = m.Claim("pod-b", 10, 15*time.Second)
	if len(claims) != 1 {
		t.Fatalf("reclaim = %d, want 1", len(claims))
	}
	if err := m.Ack(claims[0].ID, "pod-b"); err != nil {
		t.Fatal(err)
	}
	if stats := m.OutboxStats(); stats["delivered"] != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}
