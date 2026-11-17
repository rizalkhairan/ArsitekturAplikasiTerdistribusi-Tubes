package canonical

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

// These preserve the M1 read routes. Snapshot pagination, partial coverage and
// the Audit API are A09/A10; a failed distributed query returns an error here.
func (s *Store) List(ctx context.Context, source, severity, since string, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	query := `SELECT event FROM canonical.events WHERE operation='upsert'`
	args := []any{}
	for _, filter := range []struct{ column, value string }{{"source", source}, {"event->>'severity'", severity}, {"event->>'occurred_at'", since}} {
		if filter.value != "" {
			args = append(args, filter.value)
			op := "="
			if filter.column == "event->>'occurred_at'" {
				filter.column = "(event->>'occurred_at')::timestamptz"
				op = ">="
			}
			query += fmt.Sprintf(" AND %s %s $%d", filter.column, op, len(args))
		}
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY (event->>'occurred_at')::timestamptz DESC,hazard_id LIMIT $%d", len(args))
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var raw []byte
		var event Event
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}
func (s *Store) Get(ctx context.Context, id string) (Event, error) {
	var raw []byte
	var event Event
	err := s.DB.QueryRowContext(ctx, `SELECT event FROM canonical.events WHERE hazard_id=$1 AND operation='upsert'`, id).Scan(&raw)
	if err != nil {
		return event, err
	}
	err = json.Unmarshal(raw, &event)
	return event, err
}

// Publish holds a coordinator transaction lock so only one publisher sends a
// sweep. Broker acknowledgement precedes the replicated publication marker.
// A crash can repeat a send; consumers must deduplicate event_id/version.
func (s *Store) Publish(ctx context.Context, send func(context.Context, database.OutboxEvent) error) error {
	tx, _, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var acquired bool
	if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(70407001)`).Scan(&acquired); err != nil || !acquired {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT o.event_key,o.version,o.event_id,o.hazard_id,o.correlation_id,o.topic,o.payload
	 FROM canonical.outbox o WHERE o.published_at IS NULL AND o.topic=$1
	 AND NOT EXISTS(SELECT 1 FROM canonical.outbox older WHERE older.event_key=o.event_key AND older.version<o.version AND older.published_at IS NULL)
	 ORDER BY o.created_at,o.event_key LIMIT 100`, s.Topic)
	if err != nil {
		return err
	}
	type pending struct {
		key     string
		version int64
		message database.OutboxEvent
	}
	batch := []pending{}
	for rows.Next() {
		var p pending
		if err = rows.Scan(&p.key, &p.version, &p.message.EventID, &p.message.HazardID, &p.message.CorrelationID, &p.message.Topic, &p.message.Payload); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range batch {
		if err = send(ctx, p.message); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE canonical.outbox SET published_at=statement_timestamp() WHERE event_key=$1 AND version=$2`, p.key, p.version); err != nil {
			return err
		}
	}
	return tx.Commit()
}
