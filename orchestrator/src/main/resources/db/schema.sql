-- Day 8 — initial schema bootstrap.
--
-- application.yml uses hibernate.ddl-auto=validate, which checks the schema
-- matches the entities but does NOT create tables. Run this script manually
-- against your Postgres instance once, before starting Spring Boot for the
-- first time.
--
-- Run with:
--   psql -h localhost -U mlops -d mlops_pipeline -f src/main/resources/db/schema.sql
-- (enter POSTGRES_PASSWORD from your .env when prompted)

CREATE TABLE IF NOT EXISTS alerts (
    id                    BIGSERIAL PRIMARY KEY,
    machine_id            INTEGER NOT NULL,
    failure_probability   DOUBLE PRECISION NOT NULL,
    cycle_position        INTEGER,
    source_timestamp      DOUBLE PRECISION,
    received_at           TIMESTAMP NOT NULL,
    status                VARCHAR(32) NOT NULL DEFAULT 'NEW'
);

CREATE INDEX IF NOT EXISTS idx_alerts_received_at ON alerts (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_machine_id ON alerts (machine_id);

CREATE TABLE IF NOT EXISTS pending_actions (
    id                       BIGSERIAL PRIMARY KEY,
    alert_id                 BIGINT NOT NULL,
    maintenance_order_json   TEXT NOT NULL,
    status                   VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at               TIMESTAMP NOT NULL,
    resolved_at              TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pending_actions_status ON pending_actions (status);

-- Day 17 will also add a parts_inventory table here — left out today since
-- it isn't needed until the agent layer in Week 3.
