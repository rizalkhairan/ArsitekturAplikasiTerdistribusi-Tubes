CREATE TABLE IF NOT EXISTS volcano_references (
    volcano_id TEXT PRIMARY KEY NOT NULL,
    area_name TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL
) STRICT;

-- Reference for the volcano used by the PVMBG mock.
-- https://volcano.si.edu/volcano.cfm?vn=263250
INSERT INTO volcano_references (volcano_id, area_name, latitude, longitude)
VALUES ('merapi', 'Gunung Merapi', -7.54, 110.446)
ON CONFLICT (volcano_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS hazard_events (
    hazard_id TEXT PRIMARY KEY NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('BMKG', 'PVMBG')),

    source_ref_id TEXT NOT NULL,

    hazard_type TEXT NOT NULL CHECK (hazard_type IN ('SEISMIC', 'VOLCANIC')),
    severity TEXT NOT NULL CHECK (severity IN ('NORMAL', 'WASPADA', 'SIAGA', 'AWAS')),
    area_name TEXT NOT NULL,

    latitude REAL NOT NULL,
    longitude REAL NOT NULL,

    occurred_at TEXT NOT NULL,
    ingested_at TEXT NOT NULL,

    attributes TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(attributes) AND json_type(attributes) = 'object'),

    UNIQUE (source, source_ref_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_hazard_events_occurred_at
ON hazard_events (occurred_at);

-- Inclusive source timestamps, advanced only with a successful hazard batch.
CREATE TABLE IF NOT EXISTS poll_cursors (
    feed TEXT PRIMARY KEY NOT NULL,
    since TEXT NOT NULL
) STRICT;
