CREATE TABLE IF NOT EXISTS seismic_events (
    event_id TEXT PRIMARY KEY,
    magnitude REAL,
    depth_km REAL,
    epicenter_lat REAL,
    epicenter_lon REAL,
    region_name TEXT,
    occurred_at TEXT,
    potential_tsunami INTEGER
);

CREATE TABLE IF NOT EXISTS tsunami_warnings (
    warning_id TEXT PRIMARY KEY,
    related_event_id TEXT,
    threat_level TEXT,
    affected_zones TEXT,
    estimated_arrival TEXT
);

CREATE INDEX IF NOT EXISTS idx_seismic_events_occurred_at
ON seismic_events (occurred_at);

CREATE INDEX IF NOT EXISTS idx_tsunami_warnings_estimated_arrival
ON tsunami_warnings (estimated_arrival);
