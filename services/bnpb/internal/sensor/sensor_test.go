package sensor

import (
	"encoding/json"
	"testing"
	"time"
)

func stations() map[string]Station {
	return map[string]Station{"st-1": {StationID: "st-1", Name: "Pantai A", Province: "Bali", Latitude: -8.6, Longitude: 115.2}}
}

func validRaw() json.RawMessage {
	return json.RawMessage(`{
		"observation_id":"obs-1","station_id":"st-1","observed_at":"2026-11-14T10:00:00.123456Z",
		"sea_level_anomaly_m":1.8,"wave_height_m":5.2,"quality":"good","critical":true
	}`)
}

func TestValidateAcceptsContractValue(t *testing.T) {
	obs, violations := Validate(validRaw(), DefaultValidationConfig(), stations(), time.Date(2026, 11, 14, 10, 1, 0, 0, time.UTC))
	if len(violations) != 0 {
		t.Fatalf("violations = %+v", violations)
	}
	if obs.ObservationID != "obs-1" || !obs.Critical || obs.WaveHeight != 5.2 {
		t.Fatalf("obs = %+v", obs)
	}
	if got := CompositeKey(obs); got != "st-1@2026-11-14T10:00:00.123456Z#obs-1" {
		t.Fatalf("composite = %q", got)
	}
}

func TestValidateRejectsGenericGarbage(t *testing.T) {
	now := time.Date(2026, 11, 14, 10, 1, 0, 0, time.UTC)
	cases := map[string]string{
		"missing":         `{"station_id":"st-1"}`,
		"wrong type":      `{"observation_id":1,"station_id":"st-1","observed_at":"2026-11-14T10:00:00Z","sea_level_anomaly_m":1,"wave_height_m":1,"quality":"good","critical":true}`,
		"range":           `{"observation_id":"o","station_id":"st-1","observed_at":"2026-11-14T10:00:00Z","sea_level_anomaly_m":99,"wave_height_m":1,"quality":"good","critical":true}`,
		"non-utc":         `{"observation_id":"o","station_id":"st-1","observed_at":"2026-11-14T10:00:00+07:00","sea_level_anomaly_m":1,"wave_height_m":1,"quality":"good","critical":true}`,
		"future":          `{"observation_id":"o","station_id":"st-1","observed_at":"2026-11-14T12:00:00Z","sea_level_anomaly_m":1,"wave_height_m":1,"quality":"good","critical":true}`,
		"unknown station": `{"observation_id":"o","station_id":"nope","observed_at":"2026-11-14T10:00:00Z","sea_level_anomaly_m":1,"wave_height_m":1,"quality":"good","critical":true}`,
		"quality":         `{"observation_id":"o","station_id":"st-1","observed_at":"2026-11-14T10:00:00Z","sea_level_anomaly_m":1,"wave_height_m":1,"quality":"excellent","critical":true}`,
		"critical type":   `{"observation_id":"o","station_id":"st-1","observed_at":"2026-11-14T10:00:00Z","sea_level_anomaly_m":1,"wave_height_m":1,"quality":"good","critical":"yes"}`,
	}
	for name, raw := range cases {
		if _, violations := Validate(json.RawMessage(raw), DefaultValidationConfig(), stations(), now); len(violations) == 0 {
			t.Errorf("%s: expected violations", name)
		}
	}
}

type fakeNow struct{ t time.Time }

func (f *fakeNow) Now() time.Time { return f.t }

func TestBreakerTripsAndHalfOpensOneProbe(t *testing.T) {
	clock := &fakeNow{t: time.Now()}
	breaker := NewBreaker(DefaultBreakerConfig())
	breaker.SetClock(clock.Now)

	for i := 0; i < 3; i++ {
		if !breaker.Allow() {
			t.Fatalf("allow %d", i)
		}
		breaker.Failure("boom")
	}
	if state, _ := breaker.State(); state != Open {
		t.Fatalf("state = %s, want open", state)
	}
	if breaker.Allow() {
		t.Fatal("open breaker allowed a request before OpenDuration")
	}
	clock.t = clock.t.Add(16 * time.Second)
	if !breaker.Allow() {
		t.Fatal("half-open probe was not allowed")
	}
	if breaker.Allow() {
		t.Fatal("second concurrent half-open probe was allowed")
	}
	breaker.Success()
	if state, _ := breaker.State(); state != Closed {
		t.Fatalf("state after success = %s, want closed", state)
	}
}

func TestBreakerStalenessTrip(t *testing.T) {
	breaker := NewBreaker(DefaultBreakerConfig())
	breaker.Trip("staleness")
	if state, reason := breaker.State(); state != Open || reason != "staleness" {
		t.Fatalf("state=%s reason=%s", state, reason)
	}
}

func TestProgressUsesNewObservations(t *testing.T) {
	p := NewProgress()
	at := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	if !p.Observe("a", at, at) {
		t.Fatal("first observation not fresh")
	}
	if p.Observe("a", at, at) {
		t.Fatal("duplicate observation counted as fresh")
	}
	if p.Observe("b", at.Add(-time.Minute), at) {
		t.Fatal("older observation counted as fresh")
	}
	if !p.Observe("c", at.Add(time.Minute), at) {
		t.Fatal("newer observation not fresh")
	}
}

func TestSeverityMapping(t *testing.T) {
	cfg := DefaultSeverityConfig()
	cases := []struct {
		obs  Observation
		want string
	}{
		{Observation{Critical: true}, "AWAS"},
		{Observation{WaveHeight: 4.1}, "SIAGA"},
		{Observation{SeaLevelAnomaly: -1.6}, "SIAGA"},
		{Observation{WaveHeight: 2.1}, "WASPADA"},
		{Observation{SeaLevelAnomaly: 0.8}, "WASPADA"},
		{Observation{WaveHeight: 1}, "NORMAL"},
	}
	for _, c := range cases {
		if got := Severity(c.obs, cfg); got != c.want {
			t.Errorf("severity(%+v) = %s, want %s", c.obs, got, c.want)
		}
	}
}
