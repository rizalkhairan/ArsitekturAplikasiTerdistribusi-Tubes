// Package health tracks the last successful and failed upstream poll per source.
// The read API uses it to attach an explicit data-freshness marker so clients can
// tell stale Canonical Store data apart from freshly ingested data.
package health

import (
	"sync"
	"time"
)

type SourceState struct {
	LastSuccess  time.Time
	LastFailure  time.Time
	LastErrorMsg string
}

type Registry struct {
	mu     sync.RWMutex
	states map[string]SourceState
}

func New() *Registry {
	return &Registry{states: make(map[string]SourceState)}
}

func (r *Registry) RecordSuccess(source string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.states[source]
	state.LastSuccess = time.Now().UTC()
	r.states[source] = state
}

func (r *Registry) RecordFailure(source string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.states[source]
	state.LastFailure = time.Now().UTC()
	if err != nil {
		state.LastErrorMsg = err.Error()
	}
	r.states[source] = state
}

// SourceStatus reports whether a source is stale. A source is stale when its most
// recent poll failed or when no successful poll happened within maxAge.
func (r *Registry) SourceStatus(source string, maxAge time.Duration) (stale bool, lastSuccess, lastError time.Time, message string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.states[source]
	if state.LastSuccess.IsZero() {
		return true, state.LastSuccess, state.LastFailure, state.LastErrorMsg
	}
	if !state.LastFailure.IsZero() && state.LastFailure.After(state.LastSuccess) {
		return true, state.LastSuccess, state.LastFailure, state.LastErrorMsg
	}
	if time.Since(state.LastSuccess) > maxAge {
		return true, state.LastSuccess, state.LastFailure, state.LastErrorMsg
	}
	return false, state.LastSuccess, state.LastFailure, state.LastErrorMsg
}
