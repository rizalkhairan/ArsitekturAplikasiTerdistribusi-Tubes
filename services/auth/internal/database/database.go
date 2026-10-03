package database

import (
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

func Open(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize database schema: %w", err)
	}
	return db, nil
}

type User struct {
	ID       string
	Username string
	Password string
	Category string
	Scope    string
}

// SeedUsers creates users with passwords from environment variables.
// Env vars: MEDIA_USER_PASSWORD, FIELD_USER_PASSWORD, INTERNAL_USER_PASSWORD
func SeedUsers(db *sql.DB) error {
	users := []User{
		{ID: "media-user-001", Username: "media-user", Password: os.Getenv("MEDIA_USER_PASSWORD"), Category: "media", Scope: "summary"},
		{ID: "field-user-001", Username: "field-user", Password: os.Getenv("FIELD_USER_PASSWORD"), Category: "field", Scope: "full"},
		{ID: "internal-user-001", Username: "internal-user", Password: os.Getenv("INTERNAL_USER_PASSWORD"), Category: "internal", Scope: "full"},
	}

	for _, u := range users {
		if u.Password == "" {
			return fmt.Errorf("password for %s is not set (env var)", u.Username)
		}
		hashed := HashPassword(u.Password)
		_, err := db.Exec(`
			INSERT OR IGNORE INTO users (id, username, password, category, scope)
			VALUES (?, ?, ?, ?, ?)
		`, u.ID, u.Username, hashed, u.Category, u.Scope)
		if err != nil {
			return fmt.Errorf("seed user %s: %w", u.Username, err)
		}
	}
	return nil
}

// HashPassword hashes a password using SHA-256.
func HashPassword(password string) string {
	h := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", h)
}

func FindUserByUsername(db *sql.DB, username string) (User, error) {
	var u User
	err := db.QueryRow(`
		SELECT id, username, password, category, scope FROM users WHERE username = ?
	`, username).Scan(&u.ID, &u.Username, &u.Password, &u.Category, &u.Scope)
	return u, err
}

func FindUserByID(db *sql.DB, id string) (User, error) {
	var u User
	err := db.QueryRow(`
		SELECT id, username, password, category, scope FROM users WHERE id = ?
	`, id).Scan(&u.ID, &u.Username, &u.Password, &u.Category, &u.Scope)
	return u, err
}
