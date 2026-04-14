CREATE TABLE IF NOT EXISTS relay_registry (
    url_key                   TEXT        PRIMARY KEY,
    normalized_url            TEXT        NOT NULL,
    discovered_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    source_seed               BOOLEAN     NOT NULL DEFAULT FALSE,
    source_user_list          BOOLEAN     NOT NULL DEFAULT FALSE,
    source_manual             BOOLEAN     NOT NULL DEFAULT FALSE,

    manual_policy             TEXT        NOT NULL DEFAULT 'none'
        CHECK (manual_policy IN ('none', 'pinned', 'blocked', 'drained')),

    admission_state           TEXT        NOT NULL DEFAULT 'candidate'
        CHECK (admission_state IN (
            'candidate', 'probation', 'active', 'inactive',
            'blocked', 'draining', 'pinned'
        )),

    score                     DOUBLE PRECISION NOT NULL DEFAULT 0,
    distinct_user_ref_count   INTEGER     NOT NULL DEFAULT 0,
    weighted_user_ref_score   DOUBLE PRECISION NOT NULL DEFAULT 0,

    last_probe_at             TIMESTAMPTZ,
    last_probe_status         TEXT        DEFAULT 'unknown_error'
        CHECK (last_probe_status IS NULL OR last_probe_status IN (
            'ok', 'connect_failed', 'subscribe_failed', 'eose_timeout',
            'protocol_error', 'rate_limited', 'unknown_error'
        )),
    last_connect_ok           BOOLEAN,
    last_subscribe_ok         BOOLEAN,
    last_eose_ok              BOOLEAN,
    avg_connect_latency_ms    DOUBLE PRECISION,
    avg_eose_latency_ms       DOUBLE PRECISION,
    probe_fail_rate           DOUBLE PRECISION NOT NULL DEFAULT 0,
    yield_score               DOUBLE PRECISION NOT NULL DEFAULT 0,
    duplicate_ratio           DOUBLE PRECISION NOT NULL DEFAULT 0,

    score_components_json     JSONB,
    capability_summary_json   JSONB,
    notes_json                JSONB,

    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS relay_registry_normalized_url_key
    ON relay_registry (normalized_url);

CREATE INDEX IF NOT EXISTS relay_registry_admission_state_idx
    ON relay_registry (admission_state);

CREATE INDEX IF NOT EXISTS relay_registry_manual_policy_idx
    ON relay_registry (manual_policy)
    WHERE manual_policy != 'none';

CREATE INDEX IF NOT EXISTS relay_registry_last_probe_at_idx
    ON relay_registry (last_probe_at)
    WHERE last_probe_at IS NOT NULL;
