package main

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestParseAcquisitionTimeAndExactDetectionKey(t *testing.T) {
	d, err := parseDetection([]byte(`{"satellite":"N20","acq_date":"2026-11-12","acq_time":243,"latitude":-2.51230,"longitude":113.80440,"confidence":"h","frp":6.2}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := d.Key, "N20|2026-11-12|243|-2.51230|113.80440"; got != want {
		t.Fatalf("exact detection key=%q want %q", got, want)
	}
	want := time.Date(2026, 11, 12, 2, 43, 0, 0, time.UTC)
	if !d.EventTime.Equal(want) {
		t.Fatalf("event time=%s want %s", d.EventTime, want)
	}
}

func TestClusterRulesTransitiveCrossSatelliteLowConfidenceAndSingleton(t *testing.T) {
	base := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	ds := []detection{
		{Key: "N20|d|0200|-2|113", Satellite: "N20", Latitude: -2, Longitude: 113, Confidence: "h", EventTime: base},
		{Key: "N21|d|0800|-2|113.008", Satellite: "N21", Latitude: -2, Longitude: 113.008, Confidence: "n", EventTime: base.Add(6 * time.Hour)},
		{Key: "N20|d|1400|-2|113.016", Satellite: "N20", Latitude: -2, Longitude: 113.016, Confidence: "h", EventTime: base.Add(12 * time.Hour)},
		{Key: "N20|d|1400|-2|113.0001", Satellite: "N20", Latitude: -2, Longitude: 113.0001, Confidence: "low", EventTime: base.Add(12 * time.Hour)},
		{Key: "N21|d|1500|-8|120", Satellite: "N21", Latitude: -8, Longitude: 120, Confidence: "h", EventTime: base.Add(13 * time.Hour)},
	}
	components, singletons := clusterDetections(ds)
	if len(components) != 2 {
		t.Fatalf("components=%d, want fire plus singleton", len(components))
	}
	if len(components[0]) != 3 {
		t.Fatalf("transitive cross-satellite component size=%d, want 3", len(components[0]))
	}
	if singletons != 1 {
		t.Fatalf("singleton count=%d, want 1", singletons)
	}
}

func TestHaversineAndSeverityThresholds(t *testing.T) {
	if d := haversineKM(0, 0, 0, 1.0/111.195); math.Abs(d-1) > 0.001 {
		t.Fatalf("distance=%f km, want about 1 km", d)
	}
	base := time.Date(2026, 11, 12, 2, 43, 0, 0, time.UTC)
	makeDetections := func(n int, frp float64) []detection {
		out := make([]detection, n)
		for i := range out {
			out[i] = detection{Key: string(rune('a' + i)), Satellite: "N20", Latitude: -2, Longitude: 113, Confidence: "h", FRP: frp / float64(n), EventTime: base}
		}
		return out
	}
	if got := buildWildfire(makeDetections(20, 1))["severity"]; got != "AWAS" {
		t.Fatalf("count threshold severity=%v", got)
	}
	if got := buildWildfire(makeDetections(2, 100))["severity"]; got != "AWAS" {
		t.Fatalf("FRP threshold severity=%v", got)
	}
	if got := buildWildfire(makeDetections(6, 1))["severity"]; got != "SIAGA" {
		t.Fatalf("SIAGA count severity=%v", got)
	}
	if got := buildWildfire(makeDetections(2, 30))["severity"]; got != "SIAGA" {
		t.Fatalf("SIAGA FRP severity=%v", got)
	}
	if got := buildWildfire(makeDetections(2, 29.99))["severity"]; got != "WASPADA" {
		t.Fatalf("default severity=%v", got)
	}
}

func TestUpdateAndRetractInputParsing(t *testing.T) {
	update := []byte(`{"target_key":"old-key","detection":{"satellite":"N20","acq_date":"2026-11-12","acq_time":"0243","latitude":-2.5,"longitude":113.8,"confidence":"h","frp":8}}`)
	m, err := parseFeedMutation("update", update, 7)
	if err != nil {
		t.Fatal(err)
	}
	if m.Detection == nil || m.TargetKey != "old-key" || m.Detection.Key == m.TargetKey {
		t.Fatalf("unexpected update mutation: %+v", m)
	}
	retract, err := parseFeedMutation("retract", []byte(`{"detection_key":"old-key"}`), 8)
	if err != nil {
		t.Fatal(err)
	}
	if !retract.Remove || retract.TargetKey != "old-key" {
		t.Fatalf("unexpected retract: %+v", retract)
	}
}

func TestFeedOffsetPersistsRawInputAndProcessingCheckpoint(t *testing.T) {
	s, err := openStore(t.TempDir() + "/satellite.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	ctx := t.Context()
	if err := s.initializeStartOffset(ctx, 0); err != nil {
		t.Fatal(err)
	}
	data := json.RawMessage(`{"satellite":"N20","acq_date":"2026-11-12","acq_time":"0243","latitude":-2.5,"longitude":113.8,"confidence":"h","frp":10}`)
	if err := s.persistFeed(ctx, feedEvent{Offset: 0, Type: "detection", Data: data, ReceivedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.persistFeed(ctx, feedEvent{Offset: 2, Type: "heartbeat", Data: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("expected offset gap error")
	}
	var stored []byte
	if err := s.db.QueryRowContext(ctx, `SELECT payload FROM feed_events WHERE offset=0`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if string(stored) != string(data) {
		t.Fatalf("raw event changed: %s", stored)
	}
	count, err := s.processBatch(ctx, 10, 6*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("processed batch count=%d", count)
	}
	status, err := s.status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status["processed_offset"] != int64(0) {
		t.Fatalf("processed checkpoint=%v", status["processed_offset"])
	}
}

func TestStoreEmitsVersionedUpdateAndRetractForExistingFire(t *testing.T) {
	provinceData = []byte(`[{"name":"Kalimantan Tengah","latitude":-2,"longitude":113}]`)
	s, err := openStore(t.TempDir() + "/satellite.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	ctx := t.Context()
	first := []byte(`{"satellite":"N20","acq_date":"2026-11-12","acq_time":"0200","latitude":-2,"longitude":113,"confidence":"h","frp":10}`)
	second := []byte(`{"satellite":"N21","acq_date":"2026-11-12","acq_time":"0210","latitude":-2.001,"longitude":113,"confidence":"n","frp":8}`)
	update := []byte(`{"key":"N21|2026-11-12|0210|-2.001|113","changes":{"confidence":"h","frp":18,"bright_ti4":299.64}}`)
	for i, item := range []struct {
		kind string
		body []byte
	}{{"detection", first}, {"detection", second}, {"update", update}, {"retract", []byte(`{"detection_key":"N21|2026-11-12|0210|-2.001|113"}`)}, {"update", []byte(`{"key":"N21|2026-11-12|0210|-2.001|113","changes":{"frp":99}}`)}} {
		if err := s.persistFeed(ctx, feedEvent{Offset: int64(i), Type: item.kind, Data: item.body, ReceivedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.processBatch(ctx, 10, 6*time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	state, err := s.testState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(state["fires"].([]json.RawMessage)) != 0 {
		t.Fatalf("retracted singleton still has an active fire: %+v", state["fires"])
	}
	mutations := state["mutations"].([]map[string]any)
	if len(mutations) != 3 {
		t.Fatalf("mutations=%d, want initial upsert, update, retract", len(mutations))
	}
	for i, want := range []string{"upsert", "upsert", "retract"} {
		if got := mutations[i]["operation"]; got != want {
			t.Fatalf("mutation %d=%v want %s", i, got, want)
		}
	}
	if mutations[0]["source_ref_id"] != mutations[1]["source_ref_id"] || mutations[1]["source_ref_id"] != mutations[2]["source_ref_id"] {
		t.Fatal("fire identity changed during update/retraction")
	}
}

func TestOrphanPatchUpdateIsCountedAndDoesNotBlockCheckpoint(t *testing.T) {
	s, err := openStore(t.TempDir() + "/satellite.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	ctx := t.Context()
	if err := s.initializeStartOffset(ctx, 0); err != nil {
		t.Fatal(err)
	}
	data := json.RawMessage(`{"offset":0,"key":"N20|2026-11-22|557|5.62965|97.01556","changes":{"frp":4.17}}`)
	if err := s.persistFeed(ctx, feedEvent{Offset: 0, Type: "update", Data: data, ReceivedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.processBatch(ctx, 10, 6*time.Hour); err != nil || n != 1 {
		t.Fatalf("processBatch=(%d,%v), want one committed update", n, err)
	}
	status, err := s.status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status["processed_offset"] != int64(0) || status["orphan_updates"] != int64(1) {
		t.Fatalf("orphan update did not advance and count: %+v", status)
	}
}
