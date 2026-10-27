-- Profile header counters that used to be scanned at read time, or were
-- left at zero. They follow the same row as note_count: derivation keeps
-- them current, and a profile view only reads the row.
--
-- long_form_note_count is current kind-30023 addresses (one per d tag).
-- media_count is kind-1 posts with an image or video, plus kind-20 pictures.
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

-- Fill existing rows from events already stored. Later events update the
-- same columns incrementally, and a full profile-stats rebuild recomputes them.
UPDATE profile_public_stats pps
SET long_form_note_count = COALESCE(lf.cnt, 0),
    media_count = COALESCE(md.cnt, 0),
    zap_received_count = COALESCE(zr.cnt, 0),
    sats_zapped = COALESCE(zr.sats, 0),
    content_zap_count = COALESCE(zs.cnt, 0),
    time_joined = tj.min_created
FROM profile_public_stats src
LEFT JOIN (
    SELECT pubkey, COUNT(*)::bigint AS cnt
    FROM replaceable_state
    WHERE kind = 30023
    GROUP BY pubkey
) lf ON lf.pubkey = src.pubkey
LEFT JOIN (
    SELECT receiver_pubkey AS pubkey,
           COUNT(*)::bigint AS cnt,
           COALESCE(SUM(amount_sats), 0)::bigint AS sats
    FROM zap_receipts
    WHERE receiver_pubkey IS NOT NULL AND receiver_pubkey <> ''
    GROUP BY receiver_pubkey
) zr ON zr.pubkey = src.pubkey
LEFT JOIN (
    SELECT sender_pubkey AS pubkey, COUNT(*)::bigint AS cnt
    FROM zap_receipts
    GROUP BY sender_pubkey
) zs ON zs.pubkey = src.pubkey
LEFT JOIN (
    SELECT pubkey, MIN(created_at) AS min_created
    FROM events
    GROUP BY pubkey
) tj ON tj.pubkey = src.pubkey
LEFT JOIN (
    SELECT e.pubkey, COUNT(*)::bigint AS cnt
    FROM events e
    WHERE e.kind = 20
       OR (
           e.kind = 1
           AND (
               EXISTS (
                   SELECT 1
                   FROM event_tags t
                   WHERE t.event_id = e.id
                     AND LOWER(t.tag_name) IN ('image', 'thumb', 'video', 'imeta')
               )
               OR EXISTS (
                   SELECT 1
                   FROM regexp_matches(LOWER(COALESCE(e.content, '')), '(https?://[^[:space:]]+)', 'g') AS m(url)
                   WHERE url[1] ~ '\.(png|jpe?g|gif|webp|mp4|mov|webm|m4v)([?#].*)?$'
               )
           )
       )
    GROUP BY e.pubkey
) md ON md.pubkey = src.pubkey
WHERE pps.pubkey = src.pubkey;
