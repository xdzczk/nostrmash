CREATE INDEX IF NOT EXISTS idx_reaction_events_reactor_created
    ON reaction_events (reactor_pubkey, created_at DESC, event_id DESC);
