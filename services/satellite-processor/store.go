package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type store struct{ db *sql.DB }

func openStore(path string) (*store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, q := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS feed_state (
			id INTEGER PRIMARY KEY CHECK(id=1), source_offset INTEGER NOT NULL,
			processed_offset INTEGER NOT NULL, latest_offset INTEGER NOT NULL DEFAULT -1,
			max_event_time TEXT NOT NULL DEFAULT '', watermark TEXT NOT NULL DEFAULT '',
			late_count INTEGER NOT NULL DEFAULT 0, singleton_count INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '', connected INTEGER NOT NULL DEFAULT 0)`,
		`INSERT OR IGNORE INTO feed_state(id,source_offset,processed_offset,latest_offset) VALUES(1,-1,-1,-1)`,
		`CREATE TABLE IF NOT EXISTS feed_events (
			offset INTEGER PRIMARY KEY, event_type TEXT NOT NULL, payload BLOB NOT NULL,
			received_at TEXT NOT NULL, processed INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS feed_diagnostics (name TEXT PRIMARY KEY, value INTEGER NOT NULL DEFAULT 0)`,
		`INSERT OR IGNORE INTO feed_diagnostics(name,value) VALUES('orphan_updates',0)`,
		`CREATE TABLE IF NOT EXISTS detections (
			detection_key TEXT PRIMARY KEY, satellite TEXT NOT NULL, acq_date TEXT NOT NULL,
			acq_time TEXT NOT NULL, latitude REAL NOT NULL, longitude REAL NOT NULL,
			confidence TEXT NOT NULL, frp REAL NOT NULL, event_time TEXT NOT NULL,
			active INTEGER NOT NULL, last_offset INTEGER NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS detections_active_time ON detections(active,event_time)`,
		`CREATE TABLE IF NOT EXISTS clusters (
			source_ref_id TEXT PRIMARY KEY, anchor_key TEXT NOT NULL, version INTEGER NOT NULL,
			active INTEGER NOT NULL, fingerprint TEXT NOT NULL DEFAULT '', payload BLOB NOT NULL DEFAULT '{}')`,
		`CREATE TABLE IF NOT EXISTS cluster_members (
			source_ref_id TEXT NOT NULL, detection_key TEXT NOT NULL,
			PRIMARY KEY(source_ref_id,detection_key))`,
		`CREATE INDEX IF NOT EXISTS cluster_members_detection ON cluster_members(detection_key)`,
		`CREATE TABLE IF NOT EXISTS canonical_outbox (
			id INTEGER PRIMARY KEY AUTOINCREMENT, source_ref_id TEXT NOT NULL,
			version INTEGER NOT NULL, operation TEXT NOT NULL, body BLOB NOT NULL,
			created_at TEXT NOT NULL, input_received_at TEXT NOT NULL DEFAULT '', delivered_at TEXT NOT NULL DEFAULT '',
			last_error TEXT NOT NULL DEFAULT '', UNIQUE(source_ref_id,version))`,
		`CREATE TABLE IF NOT EXISTS processing_latency (
			id INTEGER PRIMARY KEY AUTOINCREMENT, seconds REAL NOT NULL, observed_at TEXT NOT NULL)`,
	} {
		if _, err := db.Exec(q); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize satellite store: %w", err)
		}
	}
	return &store{db: db}, nil
}

func (s *store) initializeStartOffset(ctx context.Context, offset int64) error {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM feed_events`).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return nil
	}
	initial := offset - 1
	_, err := s.db.ExecContext(ctx, `UPDATE feed_state SET source_offset=?,processed_offset=?,latest_offset=? WHERE id=1`, initial, initial, initial)
	return err
}

func (s *store) sourceOffset(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT source_offset FROM feed_state WHERE id=1`).Scan(&n)
	return n, err
}

func (s *store) markConnected(ctx context.Context, connected bool, errText string) {
	n := 0
	if connected {
		n = 1
	}
	_, _ = s.db.ExecContext(ctx, `UPDATE feed_state SET connected=?,last_error=? WHERE id=1`, n, errText)
}

func (s *store) persistFeed(ctx context.Context, ev feedEvent) error {
	return s.persistFeedBatch(ctx, []feedEvent{ev})
}

func (s *store) persistFeedBatch(ctx context.Context, events []feedEvent) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var last int64
	if err := tx.QueryRowContext(ctx, `SELECT source_offset FROM feed_state WHERE id=1`).Scan(&last); err != nil {
		return err
	}
	for _, ev := range events {
		if ev.Offset < 0 {
			return fmt.Errorf("feed offset must be non-negative, got %d", ev.Offset)
		}
		if ev.Offset <= last {
			var oldType string
			var oldPayload []byte
			err := tx.QueryRowContext(ctx, `SELECT event_type,payload FROM feed_events WHERE offset=?`, ev.Offset).Scan(&oldType, &oldPayload)
			if err == nil && (oldType != ev.Type || string(oldPayload) != string(ev.Data)) {
				return fmt.Errorf("feed offset %d was replayed with different content", ev.Offset)
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			continue
		}
		if ev.Offset != last+1 {
			return fmt.Errorf("feed offset gap: got %d after %d", ev.Offset, last)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO feed_events(offset,event_type,payload,received_at) VALUES(?,?,?,?)`, ev.Offset, ev.Type, []byte(ev.Data), ev.ReceivedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		last = ev.Offset
	}
	if _, err := tx.ExecContext(ctx, `UPDATE feed_state SET source_offset=?,latest_offset=MAX(latest_offset,?) WHERE id=1`, last, last); err != nil {
		return err
	}
	return tx.Commit()
}

type queuedEvent struct {
	Offset     int64
	Type       string
	Data       []byte
	ReceivedAt time.Time
}

func (s *store) processBatch(ctx context.Context, max int, allowedLateness time.Duration) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT offset,event_type,payload,received_at FROM feed_events WHERE processed=0 ORDER BY offset LIMIT ?`, max)
	if err != nil {
		return 0, err
	}
	var batch []queuedEvent
	for rows.Next() {
		var e queuedEvent
		var recv string
		if err := rows.Scan(&e.Offset, &e.Type, &e.Data, &recv); err != nil {
			rows.Close()
			return 0, err
		}
		e.ReceivedAt, err = time.Parse(time.RFC3339Nano, recv)
		if err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	if len(batch) == 0 {
		return 0, tx.Commit()
	}
	lateAdded := int64(0)
	var savedMax string
	if err := tx.QueryRowContext(ctx, `SELECT max_event_time FROM feed_state WHERE id=1`).Scan(&savedMax); err != nil {
		return 0, err
	}
	maxEvent, _ := time.Parse(time.RFC3339Nano, savedMax)
	for _, e := range batch {
		if e.Type == "heartbeat" {
			var tick struct {
				VirtualNow string `json:"virtual_now"`
			}
			if json.Unmarshal(e.Data, &tick) == nil && tick.VirtualNow != "" {
				if t, parseErr := time.Parse(time.RFC3339Nano, tick.VirtualNow); parseErr == nil && t.After(maxEvent) {
					maxEvent = t
				}
			}
			continue
		}
		m, err := parseFeedMutation(e.Type, e.Data, e.Offset)
		if err != nil {
			return 0, fmt.Errorf("offset %d: %w", e.Offset, err)
		}
		if m.Changes != nil {
			d, found, active, patchErr := applyDetectionPatch(ctx, tx, m.TargetKey, m.Changes)
			if patchErr != nil {
				return 0, fmt.Errorf("offset %d update target %q: %w", e.Offset, m.TargetKey, patchErr)
			}
			if !active {
				if !found {
					if _, err := tx.ExecContext(ctx, `UPDATE feed_diagnostics SET value=value+1 WHERE name='orphan_updates'`); err != nil {
						return 0, err
					}
				}
				continue
			}
			m.Detection = &d
		}
		if m.Detection != nil {
			d := *m.Detection
			if !maxEvent.IsZero() && d.EventTime.Before(maxEvent.Add(-allowedLateness)) {
				lateAdded++
			}
			if d.EventTime.After(maxEvent) {
				maxEvent = d.EventTime
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO detections(detection_key,satellite,acq_date,acq_time,latitude,longitude,confidence,frp,event_time,active,last_offset) VALUES(?,?,?,?,?,?,?,?,?,1,?) ON CONFLICT(detection_key) DO UPDATE SET satellite=excluded.satellite,acq_date=excluded.acq_date,acq_time=excluded.acq_time,latitude=excluded.latitude,longitude=excluded.longitude,confidence=excluded.confidence,frp=excluded.frp,event_time=excluded.event_time,active=1,last_offset=excluded.last_offset`, d.Key, d.Satellite, d.AcqDate, d.AcqTime, d.Latitude, d.Longitude, d.Confidence, d.FRP, d.EventTime.UTC().Format(time.RFC3339Nano), e.Offset); err != nil {
				return 0, err
			}
		}
		if m.TargetKey != "" && m.Detection == nil {
			if _, err := tx.ExecContext(ctx, `UPDATE detections SET active=0,last_offset=? WHERE detection_key=?`, e.Offset, m.TargetKey); err != nil {
				return 0, err
			}
		}
		if m.TargetKey != "" && m.Detection != nil && m.TargetKey != m.Detection.Key {
			if _, err := tx.ExecContext(ctx, `UPDATE detections SET active=0,last_offset=? WHERE detection_key=?`, e.Offset, m.TargetKey); err != nil {
				return 0, err
			}
		}
	}
	// Reload the transaction's active detections after applying the batch.
	var detections []detection
	rows, err = tx.QueryContext(ctx, `SELECT detection_key,satellite,acq_date,acq_time,latitude,longitude,confidence,frp,event_time FROM detections WHERE active=1 ORDER BY event_time,detection_key`)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var d detection
		var ts string
		if err := rows.Scan(&d.Key, &d.Satellite, &d.AcqDate, &d.AcqTime, &d.Latitude, &d.Longitude, &d.Confidence, &d.FRP, &ts); err != nil {
			rows.Close()
			return 0, err
		}
		d.EventTime, err = time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			rows.Close()
			return 0, err
		}
		detections = append(detections, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	components, singletons := clusterDetections(detections)
	if err := s.reconcile(ctx, tx, components, batch[len(batch)-1].ReceivedAt); err != nil {
		return 0, err
	}
	last := batch[len(batch)-1].Offset
	maxTime := maxEvent
	watermark := time.Time{}
	if !maxTime.IsZero() {
		watermark = maxTime.Add(-allowedLateness)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE feed_events SET processed=1 WHERE offset<=?`, last); err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE feed_state SET processed_offset=?,max_event_time=?,watermark=?,late_count=late_count+?,singleton_count=? WHERE id=1`, last, timeString(maxTime), timeString(watermark), lateAdded, singletons)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return len(batch), nil
}

func applyDetectionPatch(ctx context.Context, tx *sql.Tx, key string, changes map[string]json.RawMessage) (detection, bool, bool, error) {
	parts := strings.SplitN(key, "|", 5)
	if len(parts) != 5 {
		return detection{}, false, false, fmt.Errorf("invalid detection key")
	}
	var confidence string
	var frp float64
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT confidence,frp,active FROM detections WHERE detection_key=?`, key).Scan(&confidence, &frp, &active); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return detection{}, false, false, nil
		}
		return detection{}, false, false, err
	}
	if active == 0 {
		return detection{}, true, false, nil
	}
	merged := map[string]json.RawMessage{}
	for name, value := range map[string]string{"satellite": parts[0], "acq_date": parts[1], "acq_time": parts[2], "latitude": parts[3], "longitude": parts[4], "confidence": confidence} {
		encoded, _ := json.Marshal(value)
		merged[name] = encoded
	}
	encodedFRP, _ := json.Marshal(frp)
	merged["frp"] = encodedFRP
	for name, value := range changes {
		switch name {
		case "satellite", "acq_date", "acq_time", "latitude", "longitude", "confidence", "frp":
			merged[name] = value
		}
	}
	d, err := parseDetectionMap(merged)
	if err != nil {
		return detection{}, true, false, fmt.Errorf("invalid patched detection: %w", err)
	}
	return d, true, true, nil
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func (s *store) reconcile(ctx context.Context, tx *sql.Tx, comps [][]detection, received time.Time) error {
	rows, err := tx.QueryContext(ctx, `SELECT source_ref_id,anchor_key,version,fingerprint FROM clusters WHERE active=1 ORDER BY source_ref_id`)
	if err != nil {
		return err
	}
	type oldCluster struct {
		id, anchor, fingerprint string
		version                 int64
		members                 map[string]bool
	}
	var olds []*oldCluster
	for rows.Next() {
		o := &oldCluster{members: map[string]bool{}}
		if err := rows.Scan(&o.id, &o.anchor, &o.version, &o.fingerprint); err != nil {
			rows.Close()
			return err
		}
		olds = append(olds, o)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, o := range olds {
		mrows, e := tx.QueryContext(ctx, `SELECT detection_key FROM cluster_members WHERE source_ref_id=?`, o.id)
		if e != nil {
			return e
		}
		for mrows.Next() {
			var k string
			if e = mrows.Scan(&k); e != nil {
				mrows.Close()
				return e
			}
			o.members[k] = true
		}
		if e = mrows.Err(); e != nil {
			mrows.Close()
			return e
		}
		mrows.Close()
	}
	compByKey := map[string]int{}
	for i, c := range comps {
		for _, d := range c {
			compByKey[d.Key] = i
		}
	}
	assigned := make([][]*oldCluster, len(comps))
	for _, o := range olds {
		chosen := -1
		if i, ok := compByKey[o.anchor]; ok {
			chosen = i
		} else {
			best := ""
			for k := range o.members {
				if i, ok := compByKey[k]; ok {
					if best == "" || k < best {
						best = k
						chosen = i
					}
				}
			}
		}
		if chosen >= 0 {
			assigned[chosen] = append(assigned[chosen], o)
		}
	}
	usedOld := map[string]bool{}
	newIDs := make([]string, len(comps))
	for i, c := range comps {
		if len(c) < 2 {
			continue
		}
		sortDetections(c)
		if len(assigned[i]) > 0 {
			sort.Slice(assigned[i], func(a, b int) bool { return assigned[i][a].id < assigned[i][b].id })
			newIDs[i] = assigned[i][0].id
			usedOld[newIDs[i]] = true
		} else {
			newIDs[i] = stableFireID(c[0].Key)
		}
	}
	for _, o := range olds {
		if !usedOld[o.id] {
			if err := s.queueMutation(ctx, tx, o.id, o.version+1, "retract", nil, received); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE clusters SET active=0,version=? WHERE source_ref_id=?`, o.version+1, o.id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM cluster_members WHERE source_ref_id=?`, o.id); err != nil {
				return err
			}
		}
	}
	for i, c := range comps {
		if len(c) < 2 {
			continue
		}
		id := newIDs[i]
		if id == "" {
			continue
		}
		sortDetections(c)
		event := buildWildfire(c)
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		sum := sha256Hex(body)
		var priorVersion int64
		var priorFingerprint, priorAnchor string
		var priorActive, exists int
		_ = tx.QueryRowContext(ctx, `SELECT version,fingerprint,anchor_key,active FROM clusters WHERE source_ref_id=?`, id).Scan(&priorVersion, &priorFingerprint, &priorAnchor, &priorActive)
		_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM clusters WHERE source_ref_id=?`, id).Scan(&exists)
		version := priorVersion
		if exists == 0 {
			version = 0
		}
		if priorFingerprint != sum || exists == 0 || priorActive == 0 {
			version++
			if err := s.queueMutation(ctx, tx, id, version, "upsert", event, received); err != nil {
				return err
			}
		}
		anchor := c[0].Key
		if exists > 0 && priorAnchor != "" {
			anchor = priorAnchor
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO clusters(source_ref_id,anchor_key,version,active,fingerprint,payload) VALUES(?,?,?,1,?,?) ON CONFLICT(source_ref_id) DO UPDATE SET anchor_key=excluded.anchor_key,version=excluded.version,active=1,fingerprint=excluded.fingerprint,payload=excluded.payload`, id, anchor, version, sum, body); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM cluster_members WHERE source_ref_id=?`, id); err != nil {
			return err
		}
		for _, d := range c {
			if _, err := tx.ExecContext(ctx, `INSERT INTO cluster_members(source_ref_id,detection_key) VALUES(?,?)`, id, d.Key); err != nil {
				return err
			}
		}
		for _, old := range assigned[i] {
			if old.id != id {
				if err := s.queueMutation(ctx, tx, old.id, old.version+1, "retract", nil, received); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `UPDATE clusters SET active=0,version=? WHERE source_ref_id=?`, old.version+1, old.id); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `DELETE FROM cluster_members WHERE source_ref_id=?`, old.id); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *store) queueMutation(ctx context.Context, tx *sql.Tx, id string, version int64, op string, event map[string]any, received time.Time) error {
	body := map[string]any{"version": version, "operation": op, "authority": map[string]any{"partition": env("CANONICAL_PARTITION", "SATELIT"), "epoch": envInt64("CANONICAL_EPOCH", 1)}, "correlation_id": fmt.Sprintf("satellite-%s-v%d", id, version)}
	if op == "upsert" {
		body["event"] = event
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO canonical_outbox(source_ref_id,version,operation,body,created_at,input_received_at) VALUES(?,?,?,?,?,?)`, id, version, op, raw, time.Now().UTC().Format(time.RFC3339Nano), received.UTC().Format(time.RFC3339Nano))
	return err
}

type outboxRecord struct {
	ID         int64
	Ref        string
	Version    int64
	Operation  string
	Body       []byte
	ReceivedAt time.Time
}

func (s *store) nextOutbox(ctx context.Context) (outboxRecord, error) {
	var r outboxRecord
	var received string
	err := s.db.QueryRowContext(ctx, `SELECT id,source_ref_id,version,operation,body,input_received_at FROM canonical_outbox WHERE delivered_at='' ORDER BY id LIMIT 1`).Scan(&r.ID, &r.Ref, &r.Version, &r.Operation, &r.Body, &received)
	if err == nil && received != "" {
		r.ReceivedAt, _ = time.Parse(time.RFC3339Nano, received)
	}
	return r, err
}
func (s *store) markOutbox(ctx context.Context, id int64, errText string) error {
	if errText == "" {
		_, err := s.db.ExecContext(ctx, `UPDATE canonical_outbox SET delivered_at=?,last_error='' WHERE id=?`, time.Now().UTC().Format(time.RFC3339Nano), id)
		return err
	}
	_, err := s.db.ExecContext(ctx, `UPDATE canonical_outbox SET last_error=? WHERE id=?`, errText, id)
	return err
}
func (s *store) addLatency(ctx context.Context, d time.Duration) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO processing_latency(seconds,observed_at) VALUES(?,?)`, d.Seconds(), time.Now().UTC().Format(time.RFC3339Nano))
	if err == nil {
		_, _ = s.db.ExecContext(ctx, `DELETE FROM processing_latency WHERE id NOT IN (SELECT id FROM processing_latency ORDER BY id DESC LIMIT 10000)`)
	}
	return err
}
func (s *store) status(ctx context.Context) (map[string]any, error) {
	var ingested, processed, latest, late, singletons, connected int64
	var mx, wm, lastErr string
	err := s.db.QueryRowContext(ctx, `SELECT source_offset,processed_offset,latest_offset,max_event_time,watermark,late_count,singleton_count,last_error,connected FROM feed_state WHERE id=1`).Scan(&ingested, &processed, &latest, &mx, &wm, &late, &singletons, &lastErr, &connected)
	if err != nil {
		return nil, err
	}
	var pending, activeFires, activeDetections int
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM canonical_outbox WHERE delivered_at=''`).Scan(&pending)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM clusters WHERE active=1`).Scan(&activeFires)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM detections WHERE active=1`).Scan(&activeDetections)
	var p95 float64
	_ = s.db.QueryRowContext(ctx, `SELECT seconds FROM processing_latency ORDER BY seconds LIMIT 1 OFFSET (SELECT CASE WHEN COUNT(*)=0 THEN 0 ELSE CAST((COUNT(*)-1)*0.95 AS INTEGER) END FROM processing_latency)`).Scan(&p95)
	var orphanUpdates int64
	_ = s.db.QueryRowContext(ctx, `SELECT value FROM feed_diagnostics WHERE name='orphan_updates'`).Scan(&orphanUpdates)
	return map[string]any{"source_offset": ingested, "processed_offset": processed, "latest_offset": latest, "ingest_lag": latest - ingested, "processing_lag": ingested - processed, "watermark": wm, "max_event_time": mx, "late_detections": late, "singleton_detections": singletons, "orphan_updates": orphanUpdates, "active_detections": activeDetections, "active_fires": activeFires, "pending_canonical_mutations": pending, "p95_input_to_store_seconds": p95, "feed_connected": connected == 1, "last_error": lastErr}, nil
}

func (s *store) activeFireCount(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM clusters WHERE active=1`).Scan(&n)
	return n, err
}

func (s *store) resetForTest(ctx context.Context, start int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range []string{"canonical_outbox", "cluster_members", "clusters", "detections", "feed_events", "processing_latency"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE feed_diagnostics SET value=0 WHERE name='orphan_updates'`); err != nil {
		return err
	}
	initial := start - 1
	if _, err := tx.ExecContext(ctx, `UPDATE feed_state SET source_offset=?,processed_offset=?,latest_offset=?,max_event_time='',watermark='',late_count=0,singleton_count=0,last_error='',connected=0 WHERE id=1`, initial, initial, initial); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *store) testState(ctx context.Context) (map[string]any, error) {
	fires := make([]json.RawMessage, 0)
	rows, err := s.db.QueryContext(ctx, `SELECT source_ref_id,payload FROM clusters WHERE active=1 ORDER BY source_ref_id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var ref string
		var payload []byte
		if err := rows.Scan(&ref, &payload); err != nil {
			rows.Close()
			return nil, err
		}
		var fire map[string]any
		if err := json.Unmarshal(payload, &fire); err != nil {
			rows.Close()
			return nil, err
		}
		fire["source_ref_id"] = ref
		encoded, err := json.Marshal(fire)
		if err != nil {
			rows.Close()
			return nil, err
		}
		fires = append(fires, encoded)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	mutations := make([]map[string]any, 0)
	rows, err = s.db.QueryContext(ctx, `SELECT source_ref_id,version,operation,delivered_at,last_error FROM canonical_outbox ORDER BY id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var ref, op, delivered, lastErr string
		var version int64
		if err := rows.Scan(&ref, &version, &op, &delivered, &lastErr); err != nil {
			rows.Close()
			return nil, err
		}
		mutations = append(mutations, map[string]any{"source_ref_id": ref, "version": version, "operation": op, "delivered": delivered != "", "last_error": lastErr})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	return map[string]any{"fires": fires, "mutations": mutations}, nil
}

func (s *store) markError(ctx context.Context, err error) {
	if err == nil {
		return
	}
	_, _ = s.db.ExecContext(ctx, `UPDATE feed_state SET last_error=? WHERE id=1`, err.Error())
}

var errNoOutbox = errors.New("no pending outbox")
