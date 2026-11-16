package main

import (
	"errors"
	"path/filepath"
	"testing"
)

func testEvent() *HazardEvent {
	return &HazardEvent{
		Source: "SENSOR_PANTAI", SourceRefID: "station-01@2026-11-14T10:00:00Z#obs-1",
		HazardType: "COASTAL", Severity: "AWAS", AreaName: "Bali",
		Latitude: -8.65, Longitude: 115.2, OccurredAt: "2026-11-14T10:00:00Z",
		Attributes: map[string]any{"wave_height_m": 5.2},
	}
}

func TestMutateRejectsStaleFencingEpoch(t *testing.T) {
	store, err := newStore(filepath.Join(t.TempDir(), "events.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Mutate("SENSOR_PANTAI", "ref", 1, "upsert", "SENSOR_PANTAI", 5, testEvent()); err != nil {
		t.Fatalf("epoch 5: %v", err)
	}
	if _, _, _, err := store.Mutate("SENSOR_PANTAI", "ref", 1, "upsert", "SENSOR_PANTAI", 4, testEvent()); !errors.Is(err, ErrFenced) {
		t.Fatalf("epoch 4 error = %v, want ErrFenced", err)
	}
}

func TestMutateVersionRulesAndTombstone(t *testing.T) {
	store, err := newStore(filepath.Join(t.TempDir(), "events.json"))
	if err != nil {
		t.Fatal(err)
	}
	first, created, replayed, err := store.Mutate("BMKG", "ref-1", 1, "upsert", "BMKG", 1, testEvent())
	if err != nil || !created || replayed {
		t.Fatalf("first mutate: %+v %v %v %v", first, created, replayed, err)
	}
	// Identical retry is idempotent.
	_, created, replayed, err = store.Mutate("BMKG", "ref-1", 1, "upsert", "BMKG", 1, testEvent())
	if err != nil || created || !replayed {
		t.Fatalf("replay: created=%v replayed=%v err=%v", created, replayed, err)
	}
	// Lower version is rejected.
	if _, _, _, err := store.Mutate("BMKG", "ref-1", 0, "upsert", "BMKG", 1, testEvent()); !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("stale version error = %v", err)
	}
	// Higher version retract becomes a tombstone and disappears from queries.
	if _, _, _, err := store.Mutate("BMKG", "ref-1", 2, "retract", "BMKG", 1, nil); err != nil {
		t.Fatalf("retract: %v", err)
	}
	if got := len(store.Query(Query{Source: "BMKG"}).Items); got != 0 {
		t.Fatalf("active events = %d, want 0", got)
	}
	if count, _ := store.Checksum("BMKG"); count != 0 {
		t.Fatalf("checksum count = %d, want 0", count)
	}
}

func TestQueryCursorAndChecksum(t *testing.T) {
	store, _ := newStore(filepath.Join(t.TempDir(), "events.json"))
	for i := 0; i < 3; i++ {
		event := testEvent()
		event.SourceRefID = "ref-" + string(rune('a'+i))
		if _, _, _, err := store.Mutate("SENSOR_PANTAI", event.SourceRefID, 1, "upsert", "SENSOR_PANTAI", 1, event); err != nil {
			t.Fatal(err)
		}
	}
	result := store.Query(Query{Source: "SENSOR_PANTAI", Limit: 2})
	if len(result.Items) != 2 || result.Next != 2 {
		t.Fatalf("page1 = %d next=%d", len(result.Items), result.Next)
	}
	result = store.Query(Query{Source: "SENSOR_PANTAI", Limit: 2, Offset: 2})
	if len(result.Items) != 1 || result.Next != -1 {
		t.Fatalf("page2 = %d next=%d", len(result.Items), result.Next)
	}
	if count, checksum := store.Checksum("SENSOR_PANTAI"); count != 3 || len(checksum) != 64 {
		t.Fatalf("checksum count=%d value=%q", count, checksum)
	}
}
