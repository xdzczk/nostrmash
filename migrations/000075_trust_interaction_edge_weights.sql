-- Aggregated directed interaction weights for optional trust ranking signal.
-- Rebuildable from reaction_events, repost_events, zap_receipts, and thread reply
-- edges; never a product dependency on its own.
CREATE TABLE IF NOT EXISTS trust_interaction_edge_weights (
    src_pubkey TEXT NOT NULL,
    dst_pubkey TEXT NOT NULL,
    weight DOUBLE PRECISION NOT NULL CHECK (weight > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (src_pubkey, dst_pubkey)
);

CREATE INDEX IF NOT EXISTS idx_trust_interaction_edge_weights_dst
    ON trust_interaction_edge_weights (dst_pubkey);
