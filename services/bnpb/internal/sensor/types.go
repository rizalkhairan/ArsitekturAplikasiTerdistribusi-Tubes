// Sensor Pantai client: contract validation, progress health, composite identity, and last-known-good cache.
package sensor

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

const Source = "SENSOR_PANTAI"

type Station struct {
	StationID string  `json:"station_id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Observation struct {
	ObservationID   string    `json:"observation_id"`
	StationID       string    `json:"station_id"`
	ObservedAt      time.Time `json:"observed_at"`
	SeaLevelAnomaly float64   `json:"sea_level_anomaly_m"`
	WaveHeight      float64   `json:"wave_height_m"`
	Quality         string    `json:"quality"`
	Critical        bool      `json:"critical"`
}

type Page struct {
	GeneratedAt string            `json:"generated_at"`
	Items       []json.RawMessage `json:"items"`
	NextCursor  string            `json:"next_cursor"`
}

// CompositeKey survives sensor state resets; raw observation_id may repeat after /data is lost.
func CompositeKey(obs Observation) string {
	return obs.StationID + "@" + obs.ObservedAt.UTC().Format(time.RFC3339Nano) + "#" + obs.ObservationID
}

type ValidationConfig struct {
	SeaLevelMin float64
	SeaLevelMax float64
	WaveMin     float64
	WaveMax     float64
	MaxFuture   time.Duration
	MaxAge      time.Duration
	QualityEnum map[string]bool
	RequireUTC  bool
}

func DefaultValidationConfig() ValidationConfig {
	return ValidationConfig{
		SeaLevelMin: -10, SeaLevelMax: 10,
		WaveMin: 0, WaveMax: 50,
		MaxFuture: 5 * time.Minute, MaxAge: 0,
		QualityEnum: map[string]bool{
			"good": true, "fair": true, "poor": true, "degraded": true, "unknown": true,
		},
		RequireUTC: true,
	}
}

// Validation error codes are used as DLQ reasons.
const (
	ReasonSchema          = "schema"
	ReasonFieldType       = "field_type"
	ReasonNonFinite       = "non_finite"
	ReasonRange           = "range"
	ReasonTimestamp       = "timestamp"
	ReasonFutureTimestamp = "future_timestamp"
	ReasonUnknownStation  = "unknown_station"
	ReasonQuality         = "quality"
	ReasonDuplicate       = "duplicate"
)

type Violation struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

// Validate checks required fields, ranges, timestamps, station and quality.
func Validate(raw json.RawMessage, cfg ValidationConfig, stations map[string]Station, now time.Time) (Observation, []Violation) {
	var fields map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&fields); err != nil {
		return Observation{}, []Violation{{ReasonSchema, "invalid JSON object: " + err.Error()}}
	}
	var violations []Violation
	fail := func(code, detail string) { violations = append(violations, Violation{code, detail}) }

	id, ok := stringField(fields, "observation_id")
	if !ok {
		fail(ReasonSchema, "observation_id missing or not a string")
	}
	stationID, ok := stringField(fields, "station_id")
	if !ok {
		fail(ReasonSchema, "station_id missing or not a string")
	}
	observedRaw, ok := stringField(fields, "observed_at")
	if !ok {
		fail(ReasonSchema, "observed_at missing or not a string")
	}
	sea, ok := numberField(fields, "sea_level_anomaly_m")
	if !ok {
		fail(ReasonFieldType, "sea_level_anomaly_m missing or not numeric")
	}
	wave, ok := numberField(fields, "wave_height_m")
	if !ok {
		fail(ReasonFieldType, "wave_height_m missing or not numeric")
	}
	quality, ok := stringField(fields, "quality")
	if !ok {
		fail(ReasonSchema, "quality missing or not a string")
	}
	critical, ok := fields["critical"].(bool)
	if !ok {
		fail(ReasonSchema, "critical missing or not boolean")
	}

	if math.IsNaN(sea) || math.IsInf(sea, 0) || math.IsNaN(wave) || math.IsInf(wave, 0) {
		fail(ReasonNonFinite, "sea level or wave height is not finite")
	} else {
		if sea < cfg.SeaLevelMin || sea > cfg.SeaLevelMax {
			fail(ReasonRange, fmt.Sprintf("sea_level_anomaly_m %.3f outside [%g,%g]", sea, cfg.SeaLevelMin, cfg.SeaLevelMax))
		}
		if wave < cfg.WaveMin || wave > cfg.WaveMax {
			fail(ReasonRange, fmt.Sprintf("wave_height_m %.3f outside [%g,%g]", wave, cfg.WaveMin, cfg.WaveMax))
		}
	}

	var observedAt time.Time
	if observedRaw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, observedRaw)
		if err != nil {
			fail(ReasonTimestamp, "observed_at is not RFC3339: "+err.Error())
		} else {
			observedAt = parsed
			_, offset := parsed.Zone()
			if cfg.RequireUTC && offset != 0 {
				fail(ReasonTimestamp, "observed_at must be UTC")
			}
			if parsed.After(now.Add(cfg.MaxFuture)) {
				fail(ReasonFutureTimestamp, "observed_at is in the future: "+parsed.UTC().Format(time.RFC3339Nano))
			}
			if cfg.MaxAge > 0 && parsed.Before(now.Add(-cfg.MaxAge)) {
				fail(ReasonTimestamp, "observed_at is older than the configured maximum age")
			}
		}
	}
	if quality != "" && !cfg.QualityEnum[strings.ToLower(quality)] {
		fail(ReasonQuality, "unknown quality "+quality)
	}
	if stationID != "" {
		if _, ok := stations[stationID]; !ok {
			fail(ReasonUnknownStation, "station_id not present in /v1/stations")
		}
	}
	if len(violations) > 0 {
		return Observation{}, violations
	}
	return Observation{
		ObservationID: id, StationID: stationID, ObservedAt: observedAt.UTC(),
		SeaLevelAnomaly: sea, WaveHeight: wave, Quality: quality, Critical: critical,
	}, nil
}

func stringField(fields map[string]any, key string) (string, bool) {
	value, ok := fields[key].(string)
	return value, ok && strings.TrimSpace(value) != ""
}

func numberField(fields map[string]any, key string) (float64, bool) {
	switch value := fields[key].(type) {
	case json.Number:
		f, err := value.Float64()
		return f, err == nil
	case float64:
		return value, true
	default:
		return 0, false
	}
}

// Severity maps critical/thresholds to the canonical severity enum.
type SeverityConfig struct {
	SiagaWave, SiagaSea     float64
	WaspadaWave, WaspadaSea float64
}

func DefaultSeverityConfig() SeverityConfig {
	return SeverityConfig{SiagaWave: 4, SiagaSea: 1.5, WaspadaWave: 2, WaspadaSea: 0.75}
}

func Severity(obs Observation, cfg SeverityConfig) string {
	if obs.Critical {
		return "AWAS"
	}
	if obs.WaveHeight >= cfg.SiagaWave || math.Abs(obs.SeaLevelAnomaly) >= cfg.SiagaSea {
		return "SIAGA"
	}
	if obs.WaveHeight >= cfg.WaspadaWave || math.Abs(obs.SeaLevelAnomaly) >= cfg.WaspadaSea {
		return "WASPADA"
	}
	return "NORMAL"
}

// Cache is the last-known-good snapshot used for fast degraded reads.
type Cache struct {
	mu        sync.RWMutex
	stations  map[string]Station
	latest    map[string]Observation
	asOf      time.Time
	stationAt time.Time
}

func NewCache() *Cache {
	return &Cache{stations: map[string]Station{}, latest: map[string]Observation{}}
}

func (c *Cache) SetStations(stations []Station) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stations = make(map[string]Station, len(stations))
	for _, station := range stations {
		c.stations[station.StationID] = station
	}
	c.stationAt = time.Now().UTC()
}

func (c *Cache) Stations() (map[string]Station, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]Station, len(c.stations))
	for k, v := range c.stations {
		out[k] = v
	}
	return out, c.stationAt
}

func (c *Cache) Put(obs Observation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.latest[obs.StationID]; !ok || obs.ObservedAt.After(existing.ObservedAt) {
		c.latest[obs.StationID] = obs
		if obs.ObservedAt.After(c.asOf) {
			c.asOf = obs.ObservedAt
		}
	}
}

func (c *Cache) Snapshot() ([]Observation, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Observation, 0, len(c.latest))
	for _, obs := range c.latest {
		out = append(out, obs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StationID < out[j].StationID })
	return out, c.asOf
}

// Progress tracks progress-based health; /healthz is ignored.
type Progress struct {
	mu            sync.RWMutex
	seen          map[string]struct{}
	lastFresh     time.Time
	lastObserved  time.Time
	lastGenerated time.Time
}

func NewProgress() *Progress {
	return &Progress{seen: map[string]struct{}{}}
}

// Observe records a validated observation. It returns true when the observation
// is new and its observed_at is later than everything seen so far.
func (p *Progress) Observe(key string, observedAt, generatedAt time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.seen[key]; ok {
		return false
	}
	p.seen[key] = struct{}{}
	if generatedAt.After(p.lastGenerated) {
		p.lastGenerated = generatedAt
	}
	if observedAt.After(p.lastObserved) {
		p.lastObserved = observedAt
		p.lastFresh = time.Now()
		return true
	}
	return false
}

func (p *Progress) MarkFresh() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastFresh = time.Now()
}

func (p *Progress) SinceFresh(now time.Time) time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.lastFresh.IsZero() {
		return time.Duration(1<<63 - 1)
	}
	return now.Sub(p.lastFresh)
}

func (p *Progress) Snapshot() (lastFresh, lastObserved, lastGenerated time.Time, seen int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastFresh, p.lastObserved, p.lastGenerated, len(p.seen)
}
