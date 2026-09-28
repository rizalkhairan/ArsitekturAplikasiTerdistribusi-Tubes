package database

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestSaveHazardsCreatesOneOutboxEventForNewHazard(t *testing.T) {
	db := testDatabase(t)
	event := testHazard()
	if _, err := SaveHazards(context.Background(), db, []HazardEvent{event}, Cursors{SeismicFeed: event.OccurredAt.Format(time.RFC3339Nano)}, "hazard-events"); err != nil {
		t.Fatalf("SaveHazards() error = %v", err)
	}
	assertOutboxCount(t, db, 1)
}

func TestSaveHazardsDoesNotCreateOutboxEventForRepeatedHazard(t *testing.T) {
	db := testDatabase(t)
	event := testHazard()
	ctx := context.Background()
	if _, err := SaveHazards(ctx, db, []HazardEvent{event}, nil, "hazard-events"); err != nil {
		t.Fatalf("first SaveHazards() error = %v", err)
	}
	event.Severity = "SIAGA"
	if _, err := SaveHazards(ctx, db, []HazardEvent{event}, nil, "hazard-events"); err != nil {
		t.Fatalf("repeated SaveHazards() error = %v", err)
	}
	assertOutboxCount(t, db, 1)
}

func testDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "bnpb.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func testHazard() HazardEvent {
	return HazardEvent{
		Source: "BMKG", SourceRefID: "earthquake-1", HazardType: "SEISMIC",
		Severity: "NORMAL", AreaName: "Jakarta", Latitude: -6.2, Longitude: 106.8,
		OccurredAt: time.Date(2026, 9, 27, 8, 0, 0, 0, time.UTC),
		Attributes: map[string]any{"magnitude": 4.5},
	}
}

func assertOutboxCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow("SELECT COUNT(*) FROM outbox_events").Scan(&got); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	if got != want {
		t.Fatalf("outbox event count = %d, want %d", got, want)
	}
}
