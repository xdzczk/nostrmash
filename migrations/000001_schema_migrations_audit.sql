CREATE TABLE IF NOT EXISTS schema_migrations_audit (
    migration_id TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    app_version TEXT,
    checksum TEXT NOT NULL
);
