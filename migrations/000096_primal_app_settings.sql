CREATE TABLE IF NOT EXISTS primal_app_settings (
    pubkey TEXT PRIMARY KEY,
    client TEXT NOT NULL DEFAULT '',
    settings_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS primal_app_subsettings (
    pubkey TEXT NOT NULL,
    subkey TEXT NOT NULL,
    settings_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, subkey)
);
