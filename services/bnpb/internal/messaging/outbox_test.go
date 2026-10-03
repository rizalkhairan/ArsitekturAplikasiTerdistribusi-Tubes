package messaging

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

type fakePublisher struct {
	err   error
	calls int
}

func (p *fakePublisher) Publish(_ context.Context, _ database.OutboxEvent) error {
	p.calls++
	return p.err
}

func (p *fakePublisher) Close() {}

func TestOutboxRecordMarkedPublishedOnlyAfterSuccessfulPublish(t *testing.T) {
	db := outboxTestDatabase(t)
	event := createOutboxEvent(t, db)
	publisher := &fakePublisher{}
	worker := OutboxPublisher{DB: db, Publisher: publisher, RetryLimit: 1, RetryBackoff: time.Millisecond}

	if err := worker.publishOne(context.Background(), event); err != nil {
		t.Fatalf("publishOne() error = %v", err)
	}
	if publisher.calls != 1 {
		t.Fatalf("publisher calls = %d, want 1", publisher.calls)
	}
	assertPublished(t, db, event.EventID, true)
}

func TestOutboxPublishFailureLeavesRecordPending(t *testing.T) {
	db := outboxTestDatabase(t)
	event := createOutboxEvent(t, db)
	worker := OutboxPublisher{
		DB: db, Publisher: &fakePublisher{err: errors.New("broker unavailable")},
		RetryLimit: 2, RetryBackoff: time.Millisecond,
	}

	if err := worker.publishOne(context.Background(), event); err == nil {
		t.Fatal("publishOne() error = nil, want broker failure")
	}
	assertPublished(t, db, event.EventID, false)
}

func outboxTestDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db, err := database.Open(filepath.Join(t.TempDir(), "bnpb.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func createOutboxEvent(t *testing.T, db *sql.DB) database.OutboxEvent {
	t.Helper()
	hazard := database.HazardEvent{
		Source: "BMKG", SourceRefID: "earthquake-1", HazardType: "SEISMIC",
		Severity: "NORMAL", AreaName: "Jakarta", Latitude: -6.2, Longitude: 106.8,
		OccurredAt: time.Date(2026, 9, 27, 8, 0, 0, 0, time.UTC),
		Attributes: map[string]any{},
	}
	if _, err := database.SaveHazards(context.Background(), db, []database.HazardEvent{hazard}, nil, "hazard-events"); err != nil {
		t.Fatalf("SaveHazards() error = %v", err)
	}
	events, err := database.PendingOutboxEvents(context.Background(), db, 1)
	if err != nil {
		t.Fatalf("PendingOutboxEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("pending events = %d, want 1", len(events))
	}
	return events[0]
}

func assertPublished(t *testing.T, db *sql.DB, eventID string, want bool) {
	t.Helper()
	var publishedAt sql.NullString
	if err := db.QueryRow("SELECT published_at FROM outbox_events WHERE event_id = ?", eventID).Scan(&publishedAt); err != nil {
		t.Fatalf("load published state: %v", err)
	}
	if publishedAt.Valid != want {
		t.Fatalf("published = %t, want %t", publishedAt.Valid, want)
	}
}
