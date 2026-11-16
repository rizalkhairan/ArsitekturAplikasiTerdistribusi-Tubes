package sensor

import (
	"sync"
	"time"
)

type BreakerState string

const (
	Closed   BreakerState = "closed"
	Open     BreakerState = "open"
	HalfOpen BreakerState = "half_open"
)

type BreakerConfig struct {
	WindowSize        int           // outcomes kept for failure rate
	MinRequests       int           // do not trip on a tiny sample
	FailureRate       float64       // e.g. 0.5
	SlowThreshold     time.Duration // a call slower than this counts as slow
	SlowWindow        int           // trip after this many slow calls in the window
	OpenDuration      time.Duration // time before a half-open probe
	HalfOpenSuccesses int           // consecutive successes to close
}

func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		WindowSize: 10, MinRequests: 3, FailureRate: 0.5,
		SlowThreshold: 2 * time.Second, SlowWindow: 3,
		OpenDuration: 15 * time.Second, HalfOpenSuccesses: 1,
	}
}

type Breaker struct {
	mu            sync.Mutex
	cfg           BreakerConfig
	now           func() time.Time
	state         BreakerState
	results       []bool // true = failure
	slows         int
	openedAt      time.Time
	probeInFlight bool
	successes     int
	lastReason    string
	OnState       func(state BreakerState, reason string)
}

func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.WindowSize <= 0 {
		cfg = DefaultBreakerConfig()
	}
	return &Breaker{cfg: cfg, now: time.Now, state: Closed}
}

func (b *Breaker) SetClock(now func() time.Time) { b.now = now }

func (b *Breaker) State() (BreakerState, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state, b.lastReason
}

func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Closed {
		return true
	}
	if b.state == Open {
		if b.now().Sub(b.openedAt) < b.cfg.OpenDuration {
			return false
		}
		b.transition(HalfOpen, "probe")
	}
	if b.state == HalfOpen {
		if b.probeInFlight {
			return false
		}
		b.probeInFlight = true
		return true
	}
	return false
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probeInFlight = false
	if b.state == HalfOpen {
		b.successes++
		if b.successes >= b.cfg.HalfOpenSuccesses {
			b.transition(Closed, "recovered")
			b.results = nil
			b.slows = 0
		}
		return
	}
	b.record(false)
}

func (b *Breaker) Failure(reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probeInFlight = false
	if b.state == HalfOpen {
		b.transition(Open, reason)
		b.openedAt = b.now()
		return
	}
	b.record(true)
	if b.shouldTrip() {
		b.transition(Open, reason)
		b.openedAt = b.now()
	}
}

func (b *Breaker) Slow() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.slows++
	if b.state == Closed && b.slows >= b.cfg.SlowWindow {
		b.transition(Open, "slow_calls")
		b.openedAt = b.now()
	}
}

// Trip forces the breaker open, used by progress-based staleness detection.
func (b *Breaker) Trip(reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open {
		return
	}
	b.transition(Open, reason)
	b.openedAt = b.now()
}

func (b *Breaker) record(failure bool) {
	b.results = append(b.results, failure)
	if len(b.results) > b.cfg.WindowSize {
		b.results = b.results[len(b.results)-b.cfg.WindowSize:]
	}
}

func (b *Breaker) shouldTrip() bool {
	if len(b.results) < b.cfg.MinRequests {
		return false
	}
	failures := 0
	for _, failure := range b.results {
		if failure {
			failures++
		}
	}
	return float64(failures)/float64(len(b.results)) >= b.cfg.FailureRate
}

func (b *Breaker) transition(state BreakerState, reason string) {
	if b.state == state {
		return
	}
	b.state = state
	b.lastReason = reason
	b.successes = 0
	if state == HalfOpen {
		b.probeInFlight = false
	}
	if b.OnState != nil {
		b.OnState(state, reason)
	}
}
