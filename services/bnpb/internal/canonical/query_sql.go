package canonical

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

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

func (s *Store) saveSnapshot(ctx context.Context, snap snapshot) error {
	items, err := json.Marshal(snap.Items)
	if err != nil {
		return err
	}
	missing, _ := json.Marshal(snap.Missing)
	filters, _ := json.Marshal(snap.Query)
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Cache availability does not gate writes or require all physical standbys.
	// Cache loss during promotion returns an explicit snapshot_unavailable.
	_, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit='local'; SET LOCAL statement_timeout='1s'; SET LOCAL lock_timeout='500ms'`)
	if err == nil {
		_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(70409001)`)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `DELETE FROM canonical.read_snapshots WHERE expires_at < clock_timestamp()`)
	}
	if err != nil {
		return storageError("snapshot_unavailable", "snapshot storage unavailable; retry a fresh traversal")
	}
	var count, size int64
	if err = tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(octet_length(items::text)),0)+octet_length($1::jsonb::text) FROM canonical.read_snapshots`, string(items)).Scan(&count, &size); err != nil {
		return err
	}
	if count >= 64 || size > 256<<20 {
		return &Error{429, "snapshot_capacity", "snapshot cache full; retry after cursors expire"}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO canonical.read_snapshots(id,secret,filters,items,missing,expires_at) VALUES($1,$2,$3,$4,$5,$6)`, snap.ID, snap.Secret, string(filters), string(items), string(missing), snap.Expires)
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		return storageError("snapshot_unavailable", "snapshot could not be saved; retry a fresh traversal")
	}
	return nil
}

func (s *Store) loadSnapshot(ctx context.Context, id string) (snapshot, error) {
	snap := snapshot{ID: id}
	var filters, items, missing []byte
	err := s.DB.QueryRowContext(ctx, `SELECT secret,filters,items,missing,expires_at FROM canonical.read_snapshots WHERE id=$1`, id).Scan(&snap.Secret, &filters, &items, &missing, &snap.Expires)
	if errors.Is(err, sql.ErrNoRows) {
		return snap, storageError("snapshot_unavailable", "snapshot missing or evicted; start a fresh traversal")
	}
	if err != nil {
		return snap, storageError("snapshot_unavailable", "snapshot storage unavailable; retry this cursor")
	}
	if !time.Now().Before(snap.Expires) {
		return snap, &Error{400, "cursor_expired", "snapshot expired; start a fresh traversal"}
	}
	if err = json.Unmarshal(filters, &snap.Query); err == nil {
		err = json.Unmarshal(items, &snap.Items)
	}
	if err == nil {
		err = json.Unmarshal(missing, &snap.Missing)
	}
	return snap, err
}
