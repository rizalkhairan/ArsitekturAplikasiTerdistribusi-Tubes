CREATE TABLE IF NOT EXISTS volcanic_reports (
    report_id TEXT PRIMARY KEY,
    volcano_id TEXT,
    alert_level TEXT,
    eruption_count_24h INTEGER,
    ash_column_height_m REAL,
    reported_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_volcanic_reports_reported_at
ON volcanic_reports (reported_at);
