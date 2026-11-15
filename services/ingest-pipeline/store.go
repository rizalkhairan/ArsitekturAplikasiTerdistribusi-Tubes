package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type store struct{ db *sql.DB }

type failedMessage struct {
	MessageID     string    `json:"message_id"`
	Source        string    `json:"source"`
	SourceRefID   string    `json:"source_ref_id,omitempty"`
	ReceivedAt    time.Time `json:"received_at"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Reason        string    `json:"reason"`
	Retryable     bool      `json:"retryable"`
	Attempts      int       `json:"attempts"`
	Status        string    `json:"status"`
	Raw           []byte    `json:"raw"`
}

func openStore(path string) (*store, error) {
	dsn := path
	if strings.HasPrefix(path, "file:") {
		dsn += "&_pragma=busy_timeout(5000)"
	} else {
		dsn += "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	const schema = `
CREATE TABLE IF NOT EXISTS dlq_messages (
 message_id TEXT PRIMARY KEY, source TEXT NOT NULL, source_ref_id TEXT NOT NULL DEFAULT '',
 received_at TEXT NOT NULL, correlation_id TEXT NOT NULL DEFAULT '', raw BLOB NOT NULL,
 reason TEXT NOT NULL, retryable INTEGER NOT NULL, attempts INTEGER NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('retrying','parked','recovered')),
 created_at TEXT NOT NULL, recovered_at TEXT
);
CREATE INDEX IF NOT EXISTS dlq_pending_order ON dlq_messages(status, received_at, message_id);
CREATE INDEX IF NOT EXISTS dlq_source_pending ON dlq_messages(source, status, received_at, message_id);
CREATE TABLE IF NOT EXISTS field_report_outbox (
 idempotency_key TEXT PRIMARY KEY, receipt_id TEXT NOT NULL UNIQUE, payload BLOB NOT NULL,
 payload_hash TEXT NOT NULL, correlation_id TEXT NOT NULL, created_at TEXT NOT NULL,
 published_at TEXT
);
CREATE TABLE IF NOT EXISTS processed_field_reports (
 idempotency_key TEXT PRIMARY KEY, receipt_id TEXT NOT NULL, payload_hash TEXT NOT NULL,
	payload BLOB NOT NULL, processed_at TEXT NOT NULL
);
`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize pipeline store: %w", err)
	}
	return &store{db: db}, nil
}

func (s *store) park(ctx context.Context, m failedMessage) (bool, error) {
	if m.ReceivedAt.IsZero() {
		m.ReceivedAt = time.Now().UTC()
	}
	if m.Attempts < 1 {
		m.Attempts = 1
	}
	if m.MessageID == "" {
		h := sha256.New()
		h.Write([]byte(m.Source))
		h.Write([]byte{0})
		h.Write([]byte(m.SourceRefID))
		h.Write([]byte{0})
		h.Write(m.Raw)
		m.MessageID = hex.EncodeToString(h.Sum(nil))
	}
	status := "parked"
	if m.Retryable {
		status = "retrying"
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO dlq_messages(message_id,source,source_ref_id,received_at,correlation_id,raw,reason,retryable,attempts,status,created_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(message_id) DO NOTHING`, m.MessageID, m.Source, m.SourceRefID,
		m.ReceivedAt.UTC().Format(time.RFC3339Nano), m.CorrelationID, m.Raw, m.Reason, m.Retryable, m.Attempts, status, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *store) list(ctx context.Context, source, afterAt, afterID string, limit int) ([]failedMessage, bool, error) {
	query := `SELECT message_id,source,source_ref_id,received_at,correlation_id,reason,retryable,attempts,status,raw FROM dlq_messages WHERE status!='recovered'`
	args := []any{}
	if source != "" {
		query += ` AND source=?`
		args = append(args, source)
	}
	if afterAt != "" {
		query += ` AND (received_at>? OR (received_at=? AND message_id>?))`
		args = append(args, afterAt, afterAt, afterID)
	}
	query += ` ORDER BY received_at,message_id LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	items := []failedMessage{}
	for rows.Next() {
		var m failedMessage
		var at string
		if err := rows.Scan(&m.MessageID, &m.Source, &m.SourceRefID, &at, &m.CorrelationID, &m.Reason, &m.Retryable, &m.Attempts, &m.Status, &m.Raw); err != nil {
			return nil, false, err
		}
		m.ReceivedAt, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, false, err
		}
		items = append(items, m)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	return items, hasMore, nil
}

func (s *store) getFailure(ctx context.Context, id string) (failedMessage, error) {
	var m failedMessage
	var at string
	err := s.db.QueryRowContext(ctx, `SELECT message_id,source,source_ref_id,received_at,correlation_id,reason,retryable,attempts,status,raw FROM dlq_messages WHERE message_id=?`, id).
		Scan(&m.MessageID, &m.Source, &m.SourceRefID, &at, &m.CorrelationID, &m.Reason, &m.Retryable, &m.Attempts, &m.Status, &m.Raw)
	if err != nil {
		return m, err
	}
	m.ReceivedAt, err = time.Parse(time.RFC3339Nano, at)
	return m, err
}

func (s *store) listRetrying(ctx context.Context, limit int) ([]failedMessage, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT message_id,source,source_ref_id,received_at,correlation_id,reason,retryable,attempts,status,raw FROM dlq_messages WHERE status='retrying' ORDER BY received_at,message_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []failedMessage{}
	for rows.Next() {
		var m failedMessage
		var at string
		if err := rows.Scan(&m.MessageID, &m.Source, &m.SourceRefID, &at, &m.CorrelationID, &m.Reason, &m.Retryable, &m.Attempts, &m.Status, &m.Raw); err != nil {
			return nil, err
		}
		m.ReceivedAt, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, err
		}
		items = append(items, m)
	}
	return items, rows.Err()
}

func (s *store) hasEarlierPending(ctx context.Context, m failedMessage) (bool, error) {
	if m.SourceRefID == "" {
		return false, nil
	}
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dlq_messages WHERE source=? AND source_ref_id=? AND status!='recovered' AND (received_at<? OR (received_at=? AND message_id<?))`, m.Source, m.SourceRefID, m.ReceivedAt.UTC().Format(time.RFC3339Nano), m.ReceivedAt.UTC().Format(time.RFC3339Nano), m.MessageID).Scan(&count)
	return count > 0, err
}

func (s *store) markRecovered(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE dlq_messages SET status='recovered',recovered_at=COALESCE(recovered_at,?) WHERE message_id=?`, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

func (s *store) incrementAttempt(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE dlq_messages SET attempts=attempts+1 WHERE message_id=? AND status!='recovered'`, id)
	return err
}

func (s *store) acceptReport(ctx context.Context, key, receipt, corr string, payload, idempotencyBody []byte) (string, error) {
	h := sha256.Sum256(idempotencyBody)
	hash := hex.EncodeToString(h[:])
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `INSERT INTO field_report_outbox(idempotency_key,receipt_id,payload,payload_hash,correlation_id,created_at) VALUES(?,?,?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING`, key, receipt, payload, hash, corr, now)
	if err != nil {
		return "", err
	}
	var oldHash, oldReceipt string
	if err := s.db.QueryRowContext(ctx, `SELECT payload_hash,receipt_id FROM field_report_outbox WHERE idempotency_key=?`, key).Scan(&oldHash, &oldReceipt); err != nil {
		return "", err
	}
	if oldHash != hash {
		return "", errors.New("idempotency key was already used with a different report")
	}
	return oldReceipt, nil
}

func (s *store) reportOutbox(ctx context.Context, key string) ([]byte, bool, error) {
	var p []byte
	var published sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT payload,published_at FROM field_report_outbox WHERE idempotency_key=?`, key).Scan(&p, &published)
	if err != nil {
		return nil, false, err
	}
	return p, published.Valid, nil
}
func (s *store) markReportPublished(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE field_report_outbox SET published_at=? WHERE idempotency_key=? AND published_at IS NULL`, time.Now().UTC().Format(time.RFC3339Nano), key)
	return err
}

func (s *store) markReportProcessed(ctx context.Context, key, receipt string, payload []byte) (bool, error) {
	h := sha256.Sum256(payload)
	res, err := s.db.ExecContext(ctx, `INSERT INTO processed_field_reports(idempotency_key,receipt_id,payload_hash,payload,processed_at) VALUES(?,?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING`, key, receipt, hex.EncodeToString(h[:]), payload, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
