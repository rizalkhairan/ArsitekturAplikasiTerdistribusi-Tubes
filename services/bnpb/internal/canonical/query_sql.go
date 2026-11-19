package canonical

import (
	"context"
	"encoding/json"
	"fmt"
)

const maxReadRows = 20000

func (s *Store) readAllPage(ctx context.Context, q Query, pos *cursorPos, limit int) ([]copyRow, error) {
	query := `SELECT version, operation, event FROM canonical.events WHERE operation='upsert'`
	args := make([]any, 0, 5)
	if q.Source != "" {
		args = append(args, q.Source)
		query += fmt.Sprintf(" AND source=$%d", len(args))
	}
	if q.Since != "" {
		args = append(args, q.Since)
		query += fmt.Sprintf(" AND ingested_at >= $%d::timestamptz", len(args))
	}
	if pos != nil {
		args = append(args, pos.At, pos.ID)
		query += fmt.Sprintf(" AND (ingested_at, hazard_id) > ($%d::timestamptz, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY ingested_at, hazard_id LIMIT $%d", len(args))
	return s.scanRows(ctx, query, args...)
}

// readAll serves a full traversal with one Citus query. Each shard applies the
// filter and the (ingested_at, hazard_id) index supplies the order.
func (s *Store) readAll(ctx context.Context, q Query) ([]copyRow, error) {
	query := `SELECT version, operation, event FROM canonical.events WHERE operation IN ('upsert','retract')`
	args := make([]any, 0, 3)
	if q.Source != "" {
		args = append(args, q.Source)
		query += fmt.Sprintf(" AND source=$%d", len(args))
	}
	if q.Since != "" {
		args = append(args, q.Since)
		query += fmt.Sprintf(" AND ingested_at >= $%d::timestamptz", len(args))
	}
	args = append(args, maxReadRows)
	query += fmt.Sprintf(" ORDER BY ingested_at, hazard_id LIMIT $%d", len(args))
	return s.scanRows(ctx, query, args...)
}

func (s *Store) scanRows(ctx context.Context, query string, args ...any) ([]copyRow, error) {
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]copyRow, 0, 256)
	total := 0
	for rows.Next() {
		var raw []byte
		var row copyRow
		if err = rows.Scan(&row.Version, &row.Operation, &raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &row.Event); err != nil {
			return nil, err
		}
		total += len(raw)
		if total > maxReadBytes {
			return nil, fmt.Errorf("distributed read exceeds %d bytes", maxReadBytes)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) inventory(ctx context.Context) ([]unit, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT shardid,shardminvalue::bigint,shardmaxvalue::bigint
	 FROM pg_dist_shard WHERE logicalrelid='canonical.events'::regclass ORDER BY shardminvalue::bigint`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	units := []unit{}
	for rows.Next() {
		var u unit
		if err = rows.Scan(&u.ID, &u.Min, &u.Max); err != nil {
			return nil, err
		}
		units = append(units, u)
	}
	return units, rows.Err()
}

func (s *Store) readUnit(ctx context.Context, u unit, q Query) ([]copyRow, error) {
	var raw []byte
	err := s.DB.QueryRowContext(ctx, `SELECT canonical.read_shard($1,$2,$3)`, u.ID, q.Source, q.Since).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var rows []copyRow
	err = json.Unmarshal(raw, &rows)
	return rows, err
}

