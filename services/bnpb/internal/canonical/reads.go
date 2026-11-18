package canonical

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

// List preserves M1's array shape, event-time filter and newest-first order.
// Since arrays cannot express coverage, an incomplete traversal fails closed.
func (s *Store) List(ctx context.Context, source, severity, since string, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	var start time.Time
	var err error
	if since != "" {
		start, err = time.Parse(time.RFC3339Nano, since)
		if err != nil {
			return nil, err
		}
	}
	events, missing, err := gather(ctx, s, Query{Source: source})
	if err != nil {
		return nil, err
	}
	if len(missing) > 0 {
		return nil, storageError("coverage_unavailable", "legacy list cannot represent incomplete coverage")
	}
	selected := []Event{}
	for _, e := range events {
		if (severity == "" || e.Severity == severity) && !e.OccurredAt.Before(start) {
			selected = append(selected, e)
		}
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].OccurredAt.Equal(selected[j].OccurredAt) {
			return selected[i].HazardID < selected[j].HazardID
		}
		return selected[i].OccurredAt.After(selected[j].OccurredAt)
	})
	if len(selected) > limit {
		selected = selected[:limit]
	}
	return selected, nil
}
func (s *Store) Get(ctx context.Context, id string) (Event, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	var raw []byte
	var event Event
	err := s.DB.QueryRowContext(ctx, `SELECT event FROM canonical.events WHERE hazard_id=$1 AND operation='upsert'`, id).Scan(&raw)
	if err != nil {
		return event, err
	}
	err = json.Unmarshal(raw, &event)
	return event, err
}

// GetBySourceRef is the legacy read shape used by the Aggregator's store client.
func (s *Store) GetBySourceRef(ctx context.Context, ref string) (Event, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	var raw []byte
	var event Event
	err := s.DB.QueryRowContext(ctx, `SELECT event FROM canonical.events WHERE source_ref_id=$1 AND operation='upsert' ORDER BY ingested_at DESC LIMIT 1`, ref).Scan(&raw)
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
