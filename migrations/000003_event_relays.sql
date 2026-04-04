CREATE TABLE IF NOT EXISTS event_relays (
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    relay_url TEXT NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, relay_url)
);

CREATE INDEX IF NOT EXISTS idx_event_relays_relay_url ON event_relays (relay_url);
