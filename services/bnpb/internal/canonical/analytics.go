package canonical

import (
	"context"
	"fmt"
	"time"
)

// AnalyticsRow is one hourly bucket of canonical events grouped by hazard
// type, severity, and area. The dashboard aggregates on Citus so the API
// process does not transfer every event.
type AnalyticsRow struct {
	HazardType string
	Bucket     string
	Severity   string
	AreaName   string
	Count      int
}

// Analytics runs one distributed aggregate over the requested window. Citus
// fails the whole query when a placement is unreachable, so a successful
// result means every shard answered.
func (s *Store) Analytics(ctx context.Context, source string, from, to time.Time) ([]AnalyticsRow, error) {
	query := `SELECT COALESCE(event->>'hazard_type',''),
	                 to_char(date_trunc('hour', ingested_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
	                 COALESCE(event->>'severity',''),
	                 COALESCE(event->>'area_name',''),
	                 count(*)
	          FROM canonical.events
	          WHERE operation='upsert' AND ingested_at >= $1 AND ingested_at <= $2`
	args := []any{from, to}
	if source != "" {
		args = append(args, source)
		query += fmt.Sprintf(" AND source=$%d", len(args))
	}
	query += " GROUP BY 1,2,3,4 ORDER BY 2,1,3,4"

	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []AnalyticsRow{}
	for rows.Next() {
		var row AnalyticsRow
		if err = rows.Scan(&row.HazardType, &row.Bucket, &row.Severity, &row.AreaName, &row.Count); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
