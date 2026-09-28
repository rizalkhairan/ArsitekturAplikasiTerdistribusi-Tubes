package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type HazardEvent struct {
	Source      string
	SourceRefID string
	HazardType  string
	Severity    string
	AreaName    string
	Latitude    float64
	Longitude   float64
	OccurredAt  time.Time
	Attributes  map[string]any
}

// SaveHazards commits the entire batch, its new-event outbox records, and cursors together.
// An existing source record is updated but deliberately does not emit another event.
func SaveHazards(ctx context.Context, db *sql.DB, events []HazardEvent, cursors Cursors, topic string) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, event := range events {
		if err := saveHazard(ctx, tx, event, topic); err != nil {
			return 0, fmt.Errorf("%s %s: %w", event.Source, event.SourceRefID, err)
		}
	}
	for feed, since := range cursors {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO poll_cursors (feed, since) VALUES (?, ?)
			ON CONFLICT (feed) DO UPDATE SET since = excluded.since
		`, feed, since); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(events), nil
}

// Keep the original UUIDv7 and first ingestion time on repeated polls.
func saveHazard(ctx context.Context, tx *sql.Tx, event HazardEvent, topic string) error {
	if event.SourceRefID == "" || event.OccurredAt.IsZero() {
		return fmt.Errorf("source identifier and event time are required")
	}
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	attributes, err := json.Marshal(event.Attributes)
	if err != nil {
		return err
	}
	ingestedAt := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO hazard_events (
			hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			latitude, longitude, occurred_at, ingested_at, attributes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (source, source_ref_id) DO NOTHING
	`, id.String(), event.Source, event.SourceRefID, event.HazardType,
		event.Severity, event.AreaName, event.Latitude, event.Longitude,
		event.OccurredAt.UTC().Format(time.RFC3339Nano),
		ingestedAt.Format(time.RFC3339Nano), string(attributes))
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		_, err = tx.ExecContext(ctx, `
			UPDATE hazard_events SET
				hazard_type = ?, severity = ?, area_name = ?, latitude = ?, longitude = ?,
				occurred_at = ?, attributes = ?
			WHERE source = ? AND source_ref_id = ?
		`, event.HazardType, event.Severity, event.AreaName, event.Latitude, event.Longitude,
			event.OccurredAt.UTC().Format(time.RFC3339Nano), string(attributes),
			event.Source, event.SourceRefID)
		return err
	}

	correlationID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	eventID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{
		"event_id":       eventID.String(),
		"hazard_id":      id.String(),
		"source":         event.Source,
		"source_ref_id":  event.SourceRefID,
		"hazard_type":    event.HazardType,
		"severity":       event.Severity,
		"area_name":      event.AreaName,
		"latitude":       event.Latitude,
		"longitude":      event.Longitude,
		"occurred_at":    event.OccurredAt.UTC().Format(time.RFC3339Nano),
		"ingested_at":    ingestedAt.Format(time.RFC3339Nano),
		"attributes":     event.Attributes,
		"correlation_id": correlationID.String(),
	})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox_events (
			event_id, hazard_id, correlation_id, topic, payload, created_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, eventID.String(), id.String(), correlationID.String(), topic, string(payload),
		ingestedAt.Format(time.RFC3339Nano))
	return err
}

type Volcano struct {
	AreaName  string
	Latitude  float64
	Longitude float64
}

func FindVolcano(ctx context.Context, db *sql.DB, id string) (Volcano, error) {
	var volcano Volcano
	err := db.QueryRowContext(ctx, `
		SELECT area_name, latitude, longitude FROM volcano_references WHERE volcano_id = ?
	`, id).Scan(&volcano.AreaName, &volcano.Latitude, &volcano.Longitude)
	return volcano, err
}

// FindHazard retrieves canonical data needed to merge later source updates.
func FindHazard(ctx context.Context, db *sql.DB, source, refID string) (HazardEvent, error) {
	var event HazardEvent
	var occurredAt, attributes string
	err := db.QueryRowContext(ctx, `
		SELECT source, source_ref_id, hazard_type, severity, area_name,
		       latitude, longitude, occurred_at, attributes
		FROM hazard_events WHERE source = ? AND source_ref_id = ?
	`, source, refID).Scan(&event.Source, &event.SourceRefID, &event.HazardType,
		&event.Severity, &event.AreaName, &event.Latitude, &event.Longitude,
		&occurredAt, &attributes)
	if err != nil {
		return event, err
	}
	event.OccurredAt, err = time.Parse(time.RFC3339Nano, occurredAt)
	if err != nil {
		return event, err
	}
	err = json.Unmarshal([]byte(attributes), &event.Attributes)
	return event, err
}
