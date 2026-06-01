-- ══════════════════════════════════════════════════════════
-- Paris Urban Safety & Transit OSINT Map
-- PostgreSQL Schema (Production)
-- ══════════════════════════════════════════════════════════

-- PostGIS extension for spatial queries (optional but recommended)
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- ─── incidents ────────────────────────────────────────────
CREATE TABLE incidents (
    id              SERIAL PRIMARY KEY,
    uuid            VARCHAR(64)     UNIQUE NOT NULL,

    -- Location
    lat             DOUBLE PRECISION NOT NULL,
    lng             DOUBLE PRECISION NOT NULL,
    address         VARCHAR(512),
    arrondissement  SMALLINT CHECK (arrondissement BETWEEN 1 AND 20),
    -- geom         GEOMETRY(Point, 4326),  -- PostGIS point (optional)

    -- Classification
    category        CHAR(1)         NOT NULL CHECK (category IN ('A','B','C','D','E')),
    incident_type   VARCHAR(128)    NOT NULL,
    severity        SMALLINT        NOT NULL CHECK (severity BETWEEN 1 AND 5),
    description     TEXT            NOT NULL,

    -- Source
    source_url      VARCHAR(1024),
    source_type     VARCHAR(32)     NOT NULL CHECK (source_type IN ('official','news','social','telegram')),
    verified        BOOLEAN         NOT NULL DEFAULT FALSE,
    media_url       VARCHAR(1024),

    -- Timestamps
    occurred_at     TIMESTAMPTZ     NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ,
    active          BOOLEAN         NOT NULL DEFAULT TRUE,

    -- Bundling
    bundle_id       INTEGER REFERENCES incident_bundles(id) ON DELETE SET NULL,

    -- Raw content for audit / reprocessing
    raw_content     TEXT
);

CREATE INDEX idx_incidents_active     ON incidents (active);
CREATE INDEX idx_incidents_category   ON incidents (category);
CREATE INDEX idx_incidents_severity   ON incidents (severity);
CREATE INDEX idx_incidents_occurred   ON incidents (occurred_at DESC);
CREATE INDEX idx_incidents_location   ON incidents (lat, lng);
CREATE INDEX idx_incidents_verified   ON incidents (verified);
CREATE INDEX idx_incidents_bundle     ON incidents (bundle_id);

-- Trigger: auto-set updated_at
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER incidents_updated_at
BEFORE UPDATE ON incidents
FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ─── incident_bundles ─────────────────────────────────────
-- Multi-source bundled incident groups
CREATE TABLE incident_bundles (
    id              SERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    lat             DOUBLE PRECISION NOT NULL,   -- centroid
    lng             DOUBLE PRECISION NOT NULL,   -- centroid
    max_severity    SMALLINT        NOT NULL,
    source_count    SMALLINT        NOT NULL DEFAULT 1
);

CREATE INDEX idx_bundles_location ON incident_bundles (lat, lng);


-- ─── transit_alerts ───────────────────────────────────────
-- RATP / IDFM real-time disruption messages
CREATE TABLE transit_alerts (
    id                  SERIAL PRIMARY KEY,
    external_id         VARCHAR(256)    UNIQUE NOT NULL,

    line                VARCHAR(32)     NOT NULL,    -- "Métro 13", "RER B"
    line_code           VARCHAR(8)      NOT NULL,    -- "13", "B"
    direction           VARCHAR(128),
    alert_type          VARCHAR(64)     NOT NULL,    -- suspension|delay|closure|incident
    message             TEXT            NOT NULL,
    severity            SMALLINT        NOT NULL DEFAULT 2 CHECK (severity BETWEEN 1 AND 5),

    affected_stations   JSONB,                       -- ["Châtelet","Odéon",...]
    start_time          TIMESTAMPTZ     NOT NULL,
    end_time            TIMESTAMPTZ,
    active              BOOLEAN         NOT NULL DEFAULT TRUE,

    lat                 DOUBLE PRECISION,            -- representative station coords
    lng                 DOUBLE PRECISION,

    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transit_active     ON transit_alerts (active);
CREATE INDEX idx_transit_line       ON transit_alerts (line_code);
CREATE INDEX idx_transit_start      ON transit_alerts (start_time DESC);


-- ─── strikes ──────────────────────────────────────────────
-- Pre-announced strikes and union march routes
CREATE TABLE strikes (
    id              SERIAL PRIMARY KEY,
    title           VARCHAR(256)    NOT NULL,
    organizer       VARCHAR(256),               -- "CGT Transport", "FO Éducation"
    start_time      TIMESTAMPTZ     NOT NULL,
    end_time        TIMESTAMPTZ,
    severity        SMALLINT        NOT NULL DEFAULT 2 CHECK (severity BETWEEN 1 AND 5),

    -- GeoJSON polygon: march route or strike zone
    -- Format: {"type":"Polygon","coordinates":[[[lng,lat]...]]}
    route_geojson   JSONB,

    affected_lines  JSONB,                      -- ["Metro 13","RER B"]
    source_url      VARCHAR(1024),
    active          BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_strikes_active     ON strikes (active);
CREATE INDEX idx_strikes_start      ON strikes (start_time);


-- ─── ingest_log ───────────────────────────────────────────
-- Audit log for all pipeline runs
CREATE TABLE ingest_log (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source          VARCHAR(64)     NOT NULL,   -- "IDFM"|"RSS:LeMonde"|"Social"|"Manual"
    items_fetched   INTEGER         NOT NULL DEFAULT 0,
    items_added     INTEGER         NOT NULL DEFAULT 0,
    items_bundled   INTEGER         NOT NULL DEFAULT 0,
    errors          TEXT,
    duration_ms     INTEGER
);

CREATE INDEX idx_ingest_run_at ON ingest_log (run_at DESC);


-- ─── Useful Views ──────────────────────────────────────────

-- Active incidents with bundle status
CREATE VIEW v_active_incidents AS
SELECT
    i.*,
    b.source_count AS bundle_source_count,
    b.max_severity AS bundle_max_severity
FROM incidents i
LEFT JOIN incident_bundles b ON b.id = i.bundle_id
WHERE i.active = TRUE
ORDER BY i.severity DESC, i.occurred_at DESC;


-- Severity 4+ incidents in last 12 hours
CREATE VIEW v_critical_incidents AS
SELECT * FROM incidents
WHERE active = TRUE
  AND severity >= 4
  AND occurred_at >= NOW() - INTERVAL '12 hours'
ORDER BY severity DESC, occurred_at DESC;


-- Incident count by category and hour (for heat map)
CREATE VIEW v_incident_heatmap AS
SELECT
    date_trunc('hour', occurred_at) AS hour_bucket,
    category,
    COUNT(*) AS count,
    MAX(severity) AS max_severity
FROM incidents
WHERE active = TRUE
  AND occurred_at >= NOW() - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;


-- Transit alert summary
CREATE VIEW v_transit_summary AS
SELECT
    line_code,
    line,
    COUNT(*) FILTER (WHERE active) AS active_alerts,
    MAX(severity) FILTER (WHERE active) AS max_severity,
    MIN(start_time) FILTER (WHERE active) AS earliest_alert
FROM transit_alerts
GROUP BY line_code, line
ORDER BY max_severity DESC NULLS LAST;
