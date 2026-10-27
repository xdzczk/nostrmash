-- Profile header counters that used to be scanned at read time, or were
-- left at zero. They follow the same row as note_count: derivation keeps
-- them current, and a profile view only reads the row.
--
-- The cluster statement_timeout is 15s. This backfill aggregates indexed
-- counters and the discovery media flags. It does not regexp-scan event
-- bodies or walk event_tags; that scan of the events heap exceeded the
-- timeout and rolled the whole migrate transaction back.
SET LOCAL statement_timeout = '20min';

-- long_form_note_count is current kind-30023 addresses (one per d tag).
-- media_count is kind-1 posts already flagged with an image or video, plus
-- every kind-20 picture. An imeta-only note is counted when it is ingested
-- after this migration, not by this backfill.
-- zap_received_count / sats_zapped are receipts naming this pubkey.
-- content_zap_count is receipts this pubkey sent.
-- time_joined is the oldest created_at stored for this pubkey.
ALTER TABLE profile_public_stats
    ADD COLUMN IF NOT EXISTS long_form_note_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS media_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS zap_received_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sats_zapped BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS content_zap_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS time_joined BIGINT;

UPDATE profile_public_stats AS pps
SET long_form_note_count = lf.cnt
FROM (
    SELECT pubkey, COUNT(*)::bigint AS cnt
    FROM replaceable_state
    WHERE kind = 30023
    GROUP BY pubkey
) AS lf
WHERE pps.pubkey = lf.pubkey;

UPDATE profile_public_stats AS pps
SET zap_received_count = zr.cnt,
    sats_zapped = zr.sats
FROM (
    SELECT receiver_pubkey AS pubkey,
           COUNT(*)::bigint AS cnt,
           COALESCE(SUM(amount_sats), 0)::bigint AS sats
    FROM zap_receipts
    WHERE receiver_pubkey IS NOT NULL AND receiver_pubkey <> ''
    GROUP BY receiver_pubkey
) AS zr
WHERE pps.pubkey = zr.pubkey;

UPDATE profile_public_stats AS pps
SET content_zap_count = zs.cnt
FROM (
    SELECT sender_pubkey AS pubkey, COUNT(*)::bigint AS cnt
    FROM zap_receipts
    GROUP BY sender_pubkey
) AS zs
WHERE pps.pubkey = zs.pubkey;

UPDATE profile_public_stats AS pps
SET time_joined = tj.min_created
FROM (
    SELECT pubkey, MIN(created_at) AS min_created
    FROM events
    GROUP BY pubkey
) AS tj
WHERE pps.pubkey = tj.pubkey;

UPDATE profile_public_stats AS pps
SET media_count = md.cnt
FROM (
    SELECT author_pubkey AS pubkey, COUNT(*)::bigint AS cnt
    FROM note_discovery_stats
    WHERE has_image OR has_video
    GROUP BY author_pubkey
) AS md
WHERE pps.pubkey = md.pubkey;

UPDATE profile_public_stats AS pps
SET media_count = pps.media_count + pic.cnt
FROM (
    SELECT pubkey, COUNT(*)::bigint AS cnt
    FROM events
    WHERE kind = 20
    GROUP BY pubkey
) AS pic
WHERE pps.pubkey = pic.pubkey;
