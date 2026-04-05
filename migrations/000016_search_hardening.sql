CREATE EXTENSION IF NOT EXISTS pg_trgm;

ALTER TABLE profiles_latest
    ADD COLUMN IF NOT EXISTS name TEXT,
    ADD COLUMN IF NOT EXISTS display_name TEXT,
    ADD COLUMN IF NOT EXISTS about TEXT,
    ADD COLUMN IF NOT EXISTS nip05 TEXT;

UPDATE profiles_latest
SET name = COALESCE(name, profile_json ->> 'name'),
    display_name = COALESCE(display_name, profile_json ->> 'display_name'),
    about = COALESCE(about, profile_json ->> 'about'),
    nip05 = COALESCE(nip05, profile_json ->> 'nip05');

CREATE INDEX IF NOT EXISTS idx_events_kind1_content_trgm
    ON events
    USING gin (content gin_trgm_ops)
    WHERE kind = 1;

CREATE INDEX IF NOT EXISTS idx_profiles_latest_pubkey_trgm
    ON profiles_latest
    USING gin (pubkey gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_profiles_latest_search_text_trgm
    ON profiles_latest
    USING gin ((coalesce(name, '') || ' ' || coalesce(display_name, '') || ' ' || coalesce(about, '') || ' ' || coalesce(nip05, '')) gin_trgm_ops);
