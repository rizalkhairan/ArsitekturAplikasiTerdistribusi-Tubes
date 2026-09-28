package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type OutboxEvent struct {
	EventID       string
	HazardID      string
	CorrelationID string
	Topic         string
	Payload       []byte
}

func PendingOutboxEvents(ctx context.Context, db *sql.DB, limit int) ([]OutboxEvent, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT event_id, hazard_id, correlation_id, topic, payload
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at, event_id
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.EventID, &event.HazardID, &event.CorrelationID, &event.Topic, &event.Payload); err != nil {
			return nil, fmt.Errorf("scan pending outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox events: %w", err)
	}
	return events, nil
}

func MarkOutboxEventPublished(ctx context.Context, db *sql.DB, eventID string) error {
	result, err := db.ExecContext(ctx, `
		UPDATE outbox_events
		SET published_at = ?
		WHERE event_id = ? AND published_at IS NULL
	`, time.Now().UTC().Format(time.RFC3339Nano), eventID)
	if err != nil {
		return fmt.Errorf("mark outbox event %s published: %w", eventID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox publish update result: %w", err)
	}
	if updated != 1 {
		return fmt.Errorf("outbox event %s was not pending", eventID)
	}
	return nil
}
