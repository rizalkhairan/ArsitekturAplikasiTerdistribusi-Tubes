package database

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// Pools share a WAL database. Writes are serialized. Reads use their own pool.
type Pools struct {
	Read  *sql.DB
	Write *sql.DB
}

func (p *Pools) Close() error {
	return errors.Join(p.Read.Close(), p.Write.Close())
}

func OpenPools(path string, readConnections int) (*Pools, error) {
	if readConnections < 1 {
		return nil, fmt.Errorf("read connection limit must be positive")
	}
	writer, err := Open(path)
	if err != nil {
		return nil, err
	}
	dsn, err := databaseDSN(path, true)
	if err != nil {
		writer.Close()
		return nil, err
	}
	reader, err := sql.Open("sqlite", dsn)
	if err != nil {
		writer.Close()
		return nil, err
	}
	reader.SetMaxOpenConns(readConnections)
	reader.SetMaxIdleConns(readConnections)
	if err := reader.Ping(); err != nil {
		reader.Close()
		writer.Close()
		return nil, fmt.Errorf("open read pool: %w", err)
	}
	return &Pools{Read: reader, Write: writer}, nil
}

// Open initializes the schema through a single-connection write pool.
func Open(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	dsn, err := databaseDSN(path, false)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		db.Close()
		return nil, err
	}
	if mode != "wal" {
		db.Close()
		return nil, fmt.Errorf("database requires WAL mode, got %q", mode)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize database schema: %w", err)
	}
	return db, nil
}

func databaseDSN(path string, readOnly bool) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	params := url.Values{}
	// DSN pragmas apply to every connection, including connections opened later.
	params.Add("_pragma", "busy_timeout(5000)")
	if readOnly {
		params.Set("mode", "ro")
	}
	return (&url.URL{Scheme: "file", Path: absolute, RawQuery: params.Encode()}).String(), nil
}
