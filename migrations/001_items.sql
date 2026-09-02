-- Data-plane schema, applied into each run's own schema (Q2: schema-per-run).
--
-- This is the *data* plane and nothing else. Control-plane state — fault
-- config, the event log, the clock offset — never lands in Postgres
-- (HANDOFF §5 invariant 3); it lives in memory so it survives a full wipe of
-- everything below.
--
-- No `CREATE SCHEMA` here: the schema is created by the caller and set on the
-- connection's `search_path`, so these statements are schema-agnostic and the
-- same file applies to every run.

CREATE TABLE IF NOT EXISTS items (
    id         UUID PRIMARY KEY,
    name       TEXT NOT NULL,
    -- Virtual time, from the testbed clock. A row created after
    -- `clock/advance` must be able to claim a timestamp that has not happened
    -- in wall-clock terms yet, or time travel is not observable in the data.
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS items_created_at ON items (created_at DESC);
