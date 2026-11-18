// Package canonical implements the A02 write boundary inside the Aggregator.
package canonical

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

type Event struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

// Input omits store-owned identity and ingestion fields. Pointer coordinates
// distinguish a legitimate zero from a missing field.
type Input struct {
	HazardType string         `json:"hazard_type"`
	Severity   string         `json:"severity"`
	AreaName   string         `json:"area_name"`
	Latitude   *float64       `json:"latitude"`
	Longitude  *float64       `json:"longitude"`
	OccurredAt time.Time      `json:"occurred_at"`
	Attributes map[string]any `json:"attributes"`
}
type Authority struct {
	Partition string `json:"partition"`
	Epoch     int64  `json:"epoch"`
}
type Mutation struct {
	Version       int64     `json:"version"`
	Operation     string    `json:"operation"`
	Authority     Authority `json:"authority"`
	CorrelationID string    `json:"correlation_id"`
	Event         *Input    `json:"event,omitempty"`
}
type Result struct {
	HazardID   string `json:"hazard_id"`
	Version    int64  `json:"version"`
	Operation  string `json:"operation"`
	Replayed   bool   `json:"replayed"`
	Durability string `json:"durability"`
	Created    bool   `json:"-"`
}
type Lease struct {
	Authority
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expires_at"`
}

// A principal is resolved from its own Bearer credential, never from headers
// supplied by a producer. Only the authority principal may install leases.
type Principal struct {
	Owner       string              `json:"owner"`
	Assignments map[string][]string `json:"assignments"` // partition -> sources
	Authority   bool                `json:"authority,omitempty"`
}
type Error struct {
	Status        int
	Code, Message string
}

func (e *Error) Error() string     { return e.Code + ": " + e.Message }
func invalid(message string) error { return &Error{400, "invalid_request", message} }
func conflict(code string) error   { return &Error{409, code, code} }

func Key(source, ref string) string {
	encoded, _ := json.Marshal([2]string{source, ref})
	return string(encoded)
}
func hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (m Mutation) digest() string {
	return hash(struct {
		Operation string
		Event     *Input
	}{m.Operation, m.Event})
}
func (p Principal) permits(partition, source string) bool {
	for _, allowed := range p.Assignments[partition] {
		if source == allowed {
			return true
		}
	}
	return false
}
func validSource(s string) bool {
	return s == "BMKG" || s == "PVMBG" || s == "SATELIT" || s == "SENSOR_PANTAI"
}
func (m *Mutation) validate(p Principal, source, ref string) error {
	if !validSource(source) || ref == "" || len(ref) > 1024 || !utf8.ValidString(ref) || strings.ContainsAny(ref, "/\x00") {
		return invalid("invalid source or source_ref_id")
	}
	if !p.permits(m.Authority.Partition, source) {
		return &Error{403, "forbidden", "source/partition is not assigned to this producer"}
	}
	if m.Version < 1 || m.Authority.Epoch < 1 || m.CorrelationID == "" || len(m.CorrelationID) > 256 {
		return invalid("positive version/epoch and correlation_id are required")
	}
	if m.Operation == "retract" {
		if m.Event != nil {
			return invalid("retract must omit event")
		}
		return nil
	}
	if m.Operation != "upsert" || m.Event == nil {
		return invalid("upsert requires event")
	}
	e := m.Event
	if e.Latitude == nil || e.Longitude == nil || math.IsNaN(*e.Latitude) || math.IsInf(*e.Latitude, 0) || math.Abs(*e.Latitude) > 90 || math.IsNaN(*e.Longitude) || math.IsInf(*e.Longitude, 0) || math.Abs(*e.Longitude) > 180 || e.OccurredAt.IsZero() || e.AreaName == "" || e.Attributes == nil {
		return invalid("event needs coordinates, occurred_at, area_name and object attributes")
	}
	if e.HazardType != "SEISMIC" && e.HazardType != "VOLCANIC" && e.HazardType != "WILDFIRE" && e.HazardType != "COASTAL" {
		return invalid("invalid hazard_type")
	}
	if e.Severity != "NORMAL" && e.Severity != "WASPADA" && e.Severity != "SIAGA" && e.Severity != "AWAS" {
		return invalid("invalid severity")
	}
	e.OccurredAt = e.OccurredAt.UTC()
	if _, err := json.Marshal(e); err != nil {
		return invalid(fmt.Sprint(err))
	}
	return nil
}
