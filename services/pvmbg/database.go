package main

import (
	"context"
	"database/sql"
	_ "embed"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

var db *sql.DB

func openDatabase(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	if _, err := database.Exec(schema); err != nil {
		database.Close()
		return nil, err
	}
	log.Println("SQLite initialized")
	return database, nil
}

func insertReport(report VolcanicReport) {
	_, err := db.Exec(`
		INSERT INTO volcanic_reports (
			report_id,
			volcano_id,
			alert_level,
			eruption_count_24h,
			ash_column_height_m,
			reported_at
		)
		VALUES (?, ?, ?, ?, ?, ?)
	`,
		report.ReportID,
		report.VolcanoID,
		report.AlertLevel,
		report.EruptionCount24h,
		report.AshColumnHeightM,
		report.ReportedAt.Format(time.RFC3339),
	)

	if err != nil {
		log.Println(err)
	}
}

func countReports() (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM volcanic_reports").Scan(&count)
	return count, err
}

func listVolcanicReports(ctx context.Context, since string) ([]VolcanicReport, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT report_id, volcano_id, alert_level, eruption_count_24h,
		       ash_column_height_m, reported_at
		FROM volcanic_reports WHERE reported_at >= ? ORDER BY reported_at
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var reports []VolcanicReport
	for rows.Next() {
		var report VolcanicReport
		var reportedAt string
		if err := rows.Scan(&report.ReportID, &report.VolcanoID, &report.AlertLevel,
			&report.EruptionCount24h, &report.AshColumnHeightM, &reportedAt); err != nil {
			return nil, err
		}
		report.ReportedAt, err = time.Parse(time.RFC3339, reportedAt)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, rows.Err()
}
