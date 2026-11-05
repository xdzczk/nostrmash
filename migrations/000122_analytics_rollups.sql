ALTER TABLE thread_summaries
    ADD COLUMN IF NOT EXISTS direct_reply_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS first_reply_latency_s INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS branching_x100 INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS half_life_s INTEGER NOT NULL DEFAULT 0;

ALTER TABLE note_discovery_stats
    ADD COLUMN IF NOT EXISTS trusted_engagement_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS first_zap_at BIGINT;

ALTER TABLE zap_daily
    ADD COLUMN IF NOT EXISTS amount_p50_sats BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS amount_p90_sats BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS amount_mean_sats BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS gini_bps BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ttfz_p50_s BIGINT NOT NULL DEFAULT 0;

ALTER TABLE stats_snapshot_history
    ADD COLUMN IF NOT EXISTS group_messages_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS poll_votes_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS live_chat_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reports_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS deletions_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS labels_24h BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS census_events_24h BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS follower_loss_events (
    followed_pubkey TEXT NOT NULL,
    follower_pubkey TEXT NOT NULL,
    lost_at BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (followed_pubkey, follower_pubkey)
);

CREATE TABLE IF NOT EXISTS profile_graph_stats (
    pubkey TEXT PRIMARY KEY,
    mutual_count BIGINT NOT NULL DEFAULT 0,
    reciprocity_bps BIGINT NOT NULL DEFAULT 0,
    follow_back_rate_bps BIGINT NOT NULL DEFAULT 0,
    ghost_follower_bps BIGINT NOT NULL DEFAULT 0,
    mentioned_not_followed_count BIGINT NOT NULL DEFAULT 0,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS network_graph_daily (
    day DATE PRIMARY KEY,
    follower_degree_histogram JSONB NOT NULL DEFAULT '{}'::jsonb,
    mutual_edge_share_bps BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS thread_shape_daily (
    day DATE PRIMARY KEY,
    threads BIGINT NOT NULL DEFAULT 0,
    p50_first_reply_s BIGINT NOT NULL DEFAULT 0,
    avg_depth_x100 BIGINT NOT NULL DEFAULT 0,
    avg_branching_x100 BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS author_reaction_received_daily (
    pubkey TEXT NOT NULL,
    day DATE NOT NULL,
    plus_count BIGINT NOT NULL DEFAULT 0,
    minus_count BIGINT NOT NULL DEFAULT 0,
    emoji_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (pubkey, day)
);

CREATE TABLE IF NOT EXISTS zap_leaderboard_daily (
    day DATE NOT NULL,
    role TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    sats BIGINT NOT NULL DEFAULT 0,
    zap_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, role, pubkey)
);

CREATE TABLE IF NOT EXISTS zap_hourly (
    day_of_week INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    zap_count BIGINT NOT NULL DEFAULT 0,
    sats BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day_of_week, hour)
);

CREATE TABLE IF NOT EXISTS content_shape_daily (
    day DATE NOT NULL,
    kind INTEGER NOT NULL,
    length_hist JSONB NOT NULL DEFAULT '{}'::jsonb,
    mention_total BIGINT NOT NULL DEFAULT 0,
    hashtag_total BIGINT NOT NULL DEFAULT 0,
    posts BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, kind)
);

CREATE TABLE IF NOT EXISTS hashtag_daily (
    day DATE NOT NULL,
    hashtag TEXT NOT NULL,
    events BIGINT NOT NULL DEFAULT 0,
    authors BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, hashtag)
);

CREATE TABLE IF NOT EXISTS hashtag_momentum (
    hashtag TEXT PRIMARY KEY,
    z_score_7d DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT '',
    half_life_days DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kind_engagement_daily (
    day DATE NOT NULL,
    kind INTEGER NOT NULL,
    posts BIGINT NOT NULL DEFAULT 0,
    reactions BIGINT NOT NULL DEFAULT 0,
    replies BIGINT NOT NULL DEFAULT 0,
    zaps BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, kind)
);

CREATE TABLE IF NOT EXISTS deletion_daily (
    day DATE NOT NULL,
    kind INTEGER NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    latency_total_s BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, kind)
);

CREATE TABLE IF NOT EXISTS report_daily (
    day DATE NOT NULL,
    report_type TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, report_type)
);

CREATE TABLE IF NOT EXISTS report_target_counts (
    target_type TEXT NOT NULL,
    target_value TEXT NOT NULL,
    report_count BIGINT NOT NULL DEFAULT 0,
    reporter_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (target_type, target_value)
);

CREATE TABLE IF NOT EXISTS labels_daily (
    day DATE PRIMARY KEY,
    label_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mute_edges (
    muter TEXT NOT NULL,
    muted TEXT NOT NULL,
    PRIMARY KEY (muter, muted)
);

CREATE TABLE IF NOT EXISTS profile_moderation_stats (
    pubkey TEXT PRIMARY KEY,
    muted_by_count BIGINT NOT NULL DEFAULT 0,
    reports_received BIGINT NOT NULL DEFAULT 0,
    mute_rate_bps BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS content_fingerprints (
    fingerprint TEXT PRIMARY KEY,
    first_event_id TEXT NOT NULL,
    first_pubkey TEXT NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 1,
    pubkey_count BIGINT NOT NULL DEFAULT 1,
    last_seen_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS content_fingerprint_pubkeys (
    fingerprint TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    PRIMARY KEY (fingerprint, pubkey)
);

CREATE TABLE IF NOT EXISTS author_cohort_retention (
    cohort_week DATE NOT NULL,
    week_offset INTEGER NOT NULL,
    active_authors BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (cohort_week, week_offset)
);

CREATE TABLE IF NOT EXISTS relay_list_stats_daily (
    day DATE PRIMARY KEY,
    lists BIGINT NOT NULL DEFAULT 0,
    avg_relays_x100 BIGINT NOT NULL DEFAULT 0,
    p50_relays BIGINT NOT NULL DEFAULT 0,
    read_only_share_bps BIGINT NOT NULL DEFAULT 0,
    write_only_share_bps BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS relay_health_daily (
    day DATE NOT NULL,
    url_key TEXT NOT NULL,
    probes BIGINT NOT NULL DEFAULT 0,
    connect_ok_bps BIGINT NOT NULL DEFAULT 0,
    connect_p50_ms BIGINT NOT NULL DEFAULT 0,
    connect_p90_ms BIGINT NOT NULL DEFAULT 0,
    eose_p50_ms BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, url_key)
);
