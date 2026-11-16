// Canonical store service.
//
// This is the working canonical store for M2 until A delivers PostgreSQL/Citus.
// It implements the contract in contracts/m2/README.md: versioned upsert/retract,
// authoritative epoch fencing, tombstones, audit checksum/location and cursor
// queries. It is deliberately a Go service (not the Python contract stub) and
// persists to a node-local file.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	ErrStaleVersion    = errors.New("stale_version")
	ErrVersionConflict = errors.New("version_conflict")
	ErrFenced          = errors.New("fenced")
	ErrNotFound        = errors.New("not_found")
)

type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  string         `json:"occurred_at"`
	IngestedAt  string         `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

type storedEvent struct {
	Event        HazardEvent `json:"event"`
	Version      int64       `json:"version"`
	Operation    string      `json:"operation"`
	Partition    string      `json:"partition"`
	FencingEpoch int64       `json:"fencing_epoch"`
	Deleted      bool        `json:"deleted"`
	UpdatedAt    string      `json:"updated_at"`
}

type diskState struct {
	Events     map[string]storedEvent `json:"events"`
	ScopeEpoch map[string]int64       `json:"scope_epoch"`
	Sequence   int64                  `json:"sequence"`
}

type RequestLog struct {
	TS             string `json:"ts"`
	CallerInstance string `json:"caller_instance"`
	Source         string `json:"source"`
	SourceRefID    string `json:"source_ref_id"`
	Epoch          int64  `json:"fencing_epoch"`
	Version        int64  `json:"version"`
	Operation      string `json:"operation"`
	Result         string `json:"result"`
}

type Store struct {
	mu      sync.Mutex
	path    string
	state   diskState
	request []RequestLog
}

func newStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &Store{path: path, state: diskState{Events: map[string]storedEvent{}, ScopeEpoch: map[string]int64{}}}
	raw, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(raw, &s.state); err != nil {
			return nil, fmt.Errorf("load state: %w", err)
		}
		if s.state.Events == nil {
			s.state.Events = map[string]storedEvent{}
		}
		if s.state.ScopeEpoch == nil {
			s.state.ScopeEpoch = map[string]int64{}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}

func (s *Store) persistLocked() error {
	tmp := s.path + ".tmp"
	raw, err := json.MarshalIndent(s.state, "", " ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func key(source, ref string) string { return source + "\x00" + ref }

func (s *Store) recordRequest(l RequestLog) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.request = append(s.request, l)
	if len(s.request) > 5000 {
		s.request = s.request[len(s.request)-5000:]
	}
}

// Mutate applies the contract's version/operation/fencing rules.
func (s *Store) Mutate(source, ref string, version int64, operation, partition string, epoch int64, event *HazardEvent) (storedEvent, bool, bool, error) {
	return s.mutate(source, ref, version, operation, partition, epoch, event, true)
}

// MutateAdmin is used by the authenticated DLQ replay adapter. It applies the
// same version/operation rules but does not claim poller authority.
func (s *Store) MutateAdmin(source, ref string, version int64, operation string, event *HazardEvent) (storedEvent, bool, bool, error) {
	return s.mutate(source, ref, version, operation, source, 0, event, false)
}

func (s *Store) mutate(source, ref string, version int64, operation, partition string, epoch int64, event *HazardEvent, enforceFence bool) (storedEvent, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if source == "" || ref == "" {
		return storedEvent{}, false, false, fmt.Errorf("source and source_ref_id are required")
	}
	if version < 1 {
		return storedEvent{}, false, false, fmt.Errorf("%w: version must be positive", ErrStaleVersion)
	}
	if operation == "" {
		operation = "upsert"
	}
	if operation != "upsert" && operation != "retract" {
		return storedEvent{}, false, false, fmt.Errorf("operation must be upsert or retract")
	}
	if partition == "" {
		partition = source
	}
	if enforceFence {
		if max := s.state.ScopeEpoch[partition]; epoch < max {
			return storedEvent{}, false, false, fmt.Errorf("%w: epoch %d < current %d for %s", ErrFenced, epoch, max, partition)
		}
		if epoch > s.state.ScopeEpoch[partition] {
			s.state.ScopeEpoch[partition] = epoch
		}
	}

	k := key(source, ref)
	existing, had := s.state.Events[k]
	if !enforceFence && had && version <= existing.Version && operation == "upsert" {
		version = existing.Version + 1
	}
	if had {
		if version < existing.Version {
			return storedEvent{}, false, false, fmt.Errorf("%w: version %d < current %d", ErrStaleVersion, version, existing.Version)
		}
		if version == existing.Version {
			same := existing.Operation == operation && eventEqual(existing.Event, event, source, ref)
			if !same {
				return storedEvent{}, false, false, fmt.Errorf("%w: version %d reused with different content", ErrVersionConflict, version)
			}
			return existing, false, true, nil
		}
	}
	if operation == "upsert" {
		if event == nil {
			return storedEvent{}, false, false, fmt.Errorf("upsert requires event")
		}
		if event.HazardType == "" || event.Severity == "" || event.AreaName == "" || event.OccurredAt == "" {
			return storedEvent{}, false, false, fmt.Errorf("event is missing required fields")
		}
		if math.IsNaN(event.Latitude) || math.IsInf(event.Latitude, 0) ||
			math.IsNaN(event.Longitude) || math.IsInf(event.Longitude, 0) {
			return storedEvent{}, false, false, fmt.Errorf("coordinates must be finite")
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	stored := storedEvent{Version: version, Operation: operation, Partition: partition,
		FencingEpoch: epoch, UpdatedAt: now}
	if operation == "retract" {
		stored.Deleted = true
		if had {
			stored.Event = existing.Event
		} else {
			s.state.Sequence++
			stored.Event = HazardEvent{HazardID: fmt.Sprintf("hz-%012d", s.state.Sequence),
				Source: source, SourceRefID: ref, IngestedAt: now}
		}
	} else {
		stored.Event = *event
		stored.Event.Source = source
		stored.Event.SourceRefID = ref
		if had {
			stored.Event.HazardID = existing.Event.HazardID
			stored.Event.IngestedAt = existing.Event.IngestedAt
		}
		if stored.Event.HazardID == "" {
			s.state.Sequence++
			stored.Event.HazardID = fmt.Sprintf("hz-%012d", s.state.Sequence)
		}
		if stored.Event.IngestedAt == "" {
			stored.Event.IngestedAt = now
		}
	}
	s.state.Events[k] = stored
	if err := s.persistLocked(); err != nil {
		return storedEvent{}, false, false, err
	}
	return stored, !had, false, nil
}

func eventEqual(existing HazardEvent, incoming *HazardEvent, source, ref string) bool {
	if incoming == nil {
		return false
	}
	candidate := *incoming
	candidate.Source = source
	candidate.SourceRefID = ref
	if candidate.HazardID == "" {
		candidate.HazardID = existing.HazardID
	}
	if candidate.IngestedAt == "" {
		candidate.IngestedAt = existing.IngestedAt
	}
	left, _ := json.Marshal(existing)
	right, _ := json.Marshal(candidate)
	return string(left) == string(right)
}

type Query struct {
	Source string
	Since  string
	Limit  int
	Offset int
}

type QueryResult struct {
	Items []storedEvent
	Next  int
	Total int
}

func (s *Store) Query(q Query) QueryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]storedEvent, 0, len(s.state.Events))
	for _, event := range s.state.Events {
		if event.Deleted {
			continue
		}
		if q.Source != "" && event.Event.Source != q.Source {
			continue
		}
		if q.Since != "" && event.Event.IngestedAt < q.Since {
			continue
		}
		items = append(items, event)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Event.IngestedAt == items[j].Event.IngestedAt {
			return items[i].Event.HazardID < items[j].Event.HazardID
		}
		return items[i].Event.IngestedAt < items[j].Event.IngestedAt
	})
	total := len(items)
	if q.Offset < 0 {
		q.Offset = 0
	}
	if q.Offset > total {
		q.Offset = total
	}
	if q.Limit <= 0 {
		q.Limit = 100
	}
	if q.Limit > 1000 {
		q.Limit = 1000
	}
	end := q.Offset + q.Limit
	if end > total {
		end = total
	}
	next := end
	if next >= total {
		next = -1
	}
	return QueryResult{Items: items[q.Offset:end], Next: next, Total: total}
}

func (s *Store) LegacyList(source, since string, limit int) []storedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]storedEvent, 0, len(s.state.Events))
	for _, event := range s.state.Events {
		if event.Deleted {
			continue
		}
		if source != "" && event.Event.Source != source {
			continue
		}
		if since != "" && event.Event.OccurredAt < since {
			continue
		}
		items = append(items, event)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Event.OccurredAt == items[j].Event.OccurredAt {
			return items[i].Event.SourceRefID < items[j].Event.SourceRefID
		}
		return items[i].Event.OccurredAt > items[j].Event.OccurredAt
	})
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items
}

func (s *Store) Checksum(source string) (int, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	refs := make([]string, 0, len(s.state.Events))
	for _, event := range s.state.Events {
		if event.Deleted {
			continue
		}
		if source != "" && event.Event.Source != source {
			continue
		}
		refs = append(refs, event.Event.SourceRefID)
	}
	sort.Strings(refs)
	sum := sha256.New()
	for i, ref := range refs {
		if i > 0 {
			sum.Write([]byte("\n"))
		}
		sum.Write([]byte(ref))
	}
	return len(refs), hex.EncodeToString(sum.Sum(nil))
}

func (s *Store) Location(hazardID string) (storedEvent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.state.Events {
		if event.Deleted {
			continue
		}
		if event.Event.HazardID == hazardID {
			return event, true
		}
	}
	return storedEvent{}, false
}

func (s *Store) Requests() []RequestLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RequestLog, len(s.request))
	copy(out, s.request)
	return out
}

func (s *Store) Counts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := map[string]int{}
	for _, event := range s.state.Events {
		if event.Deleted {
			continue
		}
		counts[event.Event.Source]++
	}
	return counts
}

func (s *Store) location() map[string]any {
	unit := env("STORAGE_UNIT", "canonical-0")
	return map[string]any{
		"unit": unit, "node": os.Getenv("NODE_NAME"), "pod": os.Getenv("POD_NAME"),
		"storage_class": env("STORAGE_CLASS", "local-path"), "path": s.path,
	}
}
