package canonical

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

// ImportM1 reads a quiescent M1 database without modifying it. Source partitions
// and producer versions start at source/1. It preserves IDs, ingestion times,
// broker message IDs/payloads and publication state. Never run beside M1 pollers.
func (s *Store) ImportM1(ctx context.Context, path string) (int, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return 0, err
	}
	u := url.URL{Scheme: "file", Path: absolute, RawQuery: "mode=ro"}
	legacy, err := sql.Open("sqlite", u.String())
	if err != nil {
		return 0, err
	}
	defer legacy.Close()
	legacy.SetMaxOpenConns(1)
	events, err := database.ListHazards(ctx, legacy, "", "", "", 0)
	if err != nil {
		return 0, err
	}
	tx, _, err := s.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(70407002)`); err != nil {
		return 0, err
	}
	count := 0
	for _, old := range events {
		event := Event{old.HazardID, old.Source, old.SourceRefID, old.HazardType, old.Severity, old.AreaName, old.Latitude, old.Longitude, old.OccurredAt.UTC(), old.IngestedAt.UTC(), old.Attributes}
		key := Key(event.Source, event.SourceRefID)
		if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
			return 0, err
		}
		var existing string
		err = tx.QueryRowContext(ctx, `SELECT hazard_id FROM canonical.events WHERE event_key=$1`, key).Scan(&existing)
		if err == nil {
			if existing != event.HazardID {
				return 0, fmt.Errorf("identity already imported with another ID")
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		input := &Input{event.HazardType, event.Severity, event.AreaName, &event.Latitude, &event.Longitude, event.OccurredAt, event.Attributes}
		mutation := Mutation{Version: 1, Operation: "upsert", Event: input, Authority: Authority{event.Source, 1}, CorrelationID: "m1-import"}
		if err = mutation.validate(Principal{Assignments: map[string][]string{event.Source: {event.Source}}}, event.Source, event.SourceRefID); err != nil {
			return 0, err
		}
		raw, err := json.Marshal(event)
		if err != nil {
			return 0, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO canonical.events(event_key,hazard_id,source,source_ref_id,partition,version,operation,digest,event,ingested_at) VALUES($1,$2,$3,$4,$3,1,'upsert',$5,$6,$7)`, key, event.HazardID, event.Source, event.SourceRefID, mutation.digest(), string(raw), event.IngestedAt)
		if err != nil {
			return 0, err
		}
		var eventID, correlationID, topic, payload string
		var published sql.NullString
		err = legacy.QueryRowContext(ctx, `SELECT event_id,correlation_id,topic,payload,published_at FROM outbox_events WHERE hazard_id=?`, event.HazardID).Scan(&eventID, &correlationID, &topic, &payload, &published)
		if err != nil {
			return 0, fmt.Errorf("M1 event missing outbox state: %w", err)
		}
		var publishedAt any
		if published.Valid {
			parsed, err := time.Parse(time.RFC3339Nano, published.String)
			if err != nil {
				return 0, err
			}
			publishedAt = parsed
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO canonical.outbox(event_key,version,event_id,hazard_id,correlation_id,topic,payload,published_at) VALUES($1,1,$2,$3,$4,$5,$6,$7)`, key, eventID, event.HazardID, correlationID, topic, payload, publishedAt)
		if err != nil {
			return 0, err
		}
		count++
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}
