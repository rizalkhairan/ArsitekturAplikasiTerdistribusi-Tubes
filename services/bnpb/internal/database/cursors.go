package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	SeismicFeed  = "bmkg.seismic_events"    // occurred_at
	TsunamiFeed  = "bmkg.tsunami_warnings"  // estimated_arrival
	VolcanicFeed = "pvmbg.volcanic_reports" // reported_at
)

// These endpoints filter source timestamps, not modification timestamps.
// Changes older than a cursor require re-fetching history.
type Cursors map[string]string

func LoadCursors(ctx context.Context, db *sql.DB) (Cursors, error) {
	rows, err := db.QueryContext(ctx, "SELECT feed, since FROM poll_cursors")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cursors := make(Cursors)
	for rows.Next() {
		var feed, since string
		if err := rows.Scan(&feed, &since); err != nil {
			return nil, err
		}
		cursors[feed] = since
	}
	return cursors, rows.Err()
}

// Advance keeps the greatest source timestamp. Preserve its original offset
// because the mock endpoints compare their stored timestamp strings directly.
func (c Cursors) Advance(feed, timestamp string) error {
	next, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return fmt.Errorf("%s cursor: %w", feed, err)
	}
	if c[feed] != "" {
		previous, err := time.Parse(time.RFC3339Nano, c[feed])
		if err != nil {
			return err
		}
		if !next.After(previous) {
			return nil
		}
	}
	c[feed] = timestamp
	return nil
}
