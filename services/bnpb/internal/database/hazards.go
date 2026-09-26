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

// SaveHazards commits the entire batch and its cursors together.
func SaveHazards(ctx context.Context, db *sql.DB, events []HazardEvent, cursors Cursors) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, event := range events {
		if err := saveHazard(ctx, tx, event); err != nil {
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
func saveHazard(ctx context.Context, tx *sql.Tx, event HazardEvent) error {
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
	_, err = tx.ExecContext(ctx, `
		INSERT INTO hazard_events (
			hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			latitude, longitude, occurred_at, ingested_at, attributes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (source, source_ref_id) DO UPDATE SET
			hazard_type = excluded.hazard_type,
			severity = excluded.severity,
			area_name = excluded.area_name,
			latitude = excluded.latitude,
			longitude = excluded.longitude,
			occurred_at = excluded.occurred_at,
			attributes = excluded.attributes
	`, id.String(), event.Source, event.SourceRefID, event.HazardType,
		event.Severity, event.AreaName, event.Latitude, event.Longitude,
		event.OccurredAt.UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano), string(attributes))
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
