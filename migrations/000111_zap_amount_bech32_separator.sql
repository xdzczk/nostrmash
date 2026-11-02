-- Migration 000108 recovered a missing zap amount by splitting the BOLT11
-- invoice on the first '1'. The amount often contains that digit
-- (lnbc210n is 21 sats), so the cut dropped the multiplier and the
-- leftover number was stored as whole BTC. A 21-sat receipt became
-- 2 BTC, and those rows own the most-zapped lists.
--
-- Recompute every receipt with the same rules as zapReceiptAmountSats:
-- the receipt amount tag, then the amount tag inside the description
-- zap request, then the BOLT11 amount. The invoice separator is the
-- last '1'. Then rebuild the note totals and the paid ranking tables
-- from the corrected column. The worker rewrites the ranking tables
-- on its next tick; this pass makes them correct before that.
--
-- Production statement_timeout is 15s. Parsing every receipt does not
-- finish in that window. This setting lasts only for this transaction.
SET LOCAL statement_timeout = 0;

CREATE OR REPLACE FUNCTION migration_000111_parse_msat_sats(raw text)
RETURNS bigint
LANGUAGE plpgsql
STABLE
AS $fn$
DECLARE
    amount bigint;
BEGIN
    IF raw IS NULL THEN
        RETURN 0;
    END IF;
    raw := btrim(raw);
    IF raw = '' OR raw !~ '^[0-9]+$' THEN
        RETURN 0;
    END IF;
    BEGIN
        amount := raw::bigint;
    EXCEPTION
        WHEN others THEN
            RETURN 0;
    END;
    IF amount <= 0 THEN
        RETURN 0;
    END IF;
    IF amount >= 1000 THEN
        RETURN amount / 1000;
    END IF;
    RETURN amount;
END;
$fn$;

CREATE OR REPLACE FUNCTION migration_000111_zap_amount_sats(raw jsonb)
RETURNS bigint
LANGUAGE plpgsql
STABLE
AS $fn$
DECLARE
    tag jsonb;
    inner_tag jsonb;
    request jsonb;
    amount_text text;
    parsed bigint;
    bolt11 text;
    hrp text;
    amount text;
    mult text;
    scaled numeric;
BEGIN
    IF raw IS NULL OR jsonb_typeof(raw->'tags') <> 'array' THEN
        RETURN 0;
    END IF;

    amount_text := NULL;
    FOR tag IN
        SELECT value FROM jsonb_array_elements(raw->'tags')
    LOOP
        IF jsonb_typeof(tag) = 'array'
           AND jsonb_array_length(tag) >= 2
           AND tag->>0 = 'amount' THEN
            amount_text := tag->>1;
            EXIT;
        END IF;
    END LOOP;
    parsed := migration_000111_parse_msat_sats(amount_text);
    IF parsed > 0 THEN
        RETURN parsed;
    END IF;

    FOR tag IN
        SELECT value FROM jsonb_array_elements(raw->'tags')
    LOOP
        IF jsonb_typeof(tag) <> 'array'
           OR jsonb_array_length(tag) < 2
           OR tag->>0 <> 'description' THEN
            CONTINUE;
        END IF;
        BEGIN
            request := (tag->>1)::jsonb;
        EXCEPTION
            WHEN others THEN
                CONTINUE;
        END;
        IF jsonb_typeof(request) <> 'object' OR jsonb_typeof(request->'tags') <> 'array' THEN
            CONTINUE;
        END IF;
        amount_text := NULL;
        FOR inner_tag IN
            SELECT value FROM jsonb_array_elements(request->'tags')
        LOOP
            IF jsonb_typeof(inner_tag) = 'array'
               AND jsonb_array_length(inner_tag) >= 2
               AND inner_tag->>0 = 'amount' THEN
                amount_text := inner_tag->>1;
                EXIT;
            END IF;
        END LOOP;
        parsed := migration_000111_parse_msat_sats(amount_text);
        IF parsed > 0 THEN
            RETURN parsed;
        END IF;
    END LOOP;

    bolt11 := NULL;
    FOR tag IN
        SELECT value FROM jsonb_array_elements(raw->'tags')
    LOOP
        IF jsonb_typeof(tag) = 'array'
           AND jsonb_array_length(tag) >= 2
           AND tag->>0 = 'bolt11' THEN
            bolt11 := tag->>1;
            EXIT;
        END IF;
    END LOOP;
    IF bolt11 IS NULL THEN
        RETURN 0;
    END IF;
    bolt11 := btrim(bolt11);
    IF bolt11 = '' OR position('1' IN bolt11) = 0 THEN
        RETURN 0;
    END IF;
    -- The bech32 separator is the last '1'. An earlier '1' is a digit in
    -- the human-readable amount.
    hrp := lower(left(bolt11, length(bolt11) - position('1' IN reverse(bolt11))));
    IF left(hrp, 6) = 'lnbcrt' THEN
        amount := substr(hrp, 7);
    ELSIF left(hrp, 4) = 'lnbc' THEN
        amount := substr(hrp, 5);
    ELSIF left(hrp, 4) = 'lntb' THEN
        amount := substr(hrp, 5);
    ELSE
        RETURN 0;
    END IF;
    IF amount = '' THEN
        RETURN 0;
    END IF;
    mult := right(amount, 1);
    IF mult IN ('m', 'u', 'n', 'p') THEN
        amount := left(amount, length(amount) - 1);
    ELSE
        mult := '';
    END IF;
    IF amount = '' OR amount !~ '^[0-9]*\.?[0-9]+$' THEN
        RETURN 0;
    END IF;
    BEGIN
        scaled := CASE mult
            WHEN 'm' THEN amount::numeric * 100000
            WHEN 'u' THEN amount::numeric * 100
            WHEN 'n' THEN amount::numeric / 10
            WHEN 'p' THEN amount::numeric / 10000
            ELSE amount::numeric * 100000000
        END;
    EXCEPTION
        WHEN others THEN
            RETURN 0;
    END;
    IF scaled IS NULL OR scaled < 1 THEN
        RETURN 0;
    END IF;
    BEGIN
        RETURN floor(scaled)::bigint;
    EXCEPTION
        WHEN others THEN
            RETURN 0;
    END;
END;
$fn$;

UPDATE zap_receipts AS z
SET amount_sats = parsed.sats
FROM (
    SELECT e.id AS zap_receipt_id, migration_000111_zap_amount_sats(e.raw_json) AS sats
    FROM events e
    WHERE e.kind = 9735
) AS parsed
WHERE z.zap_receipt_id = parsed.zap_receipt_id
  AND z.amount_sats IS DISTINCT FROM parsed.sats;

UPDATE note_discovery_stats AS stats
SET zap_msats = computed.zap_msats
FROM (
    SELECT
        z.event_id,
        COALESCE(SUM(z.amount_sats), 0)::bigint * 1000 AS zap_msats
    FROM zap_receipts z
    WHERE z.event_id IS NOT NULL
      AND z.event_id <> ''
    GROUP BY z.event_id
) AS computed
WHERE stats.event_id = computed.event_id
  AND stats.zap_msats IS DISTINCT FROM computed.zap_msats;

UPDATE note_discovery_stats AS stats
SET zap_msats = computed.zap_msats
FROM (
    SELECT
        rs.event_id,
        COALESCE(SUM(z.amount_sats), 0)::bigint * 1000 AS zap_msats
    FROM replaceable_state rs
    JOIN zap_receipts z ON (
        z.event_id = rs.event_id
        OR (
            z.target_address <> ''
            AND z.target_address = (rs.kind::text || ':' || lower(rs.pubkey) || ':' || rs.d_tag)
            AND z.event_id IS DISTINCT FROM rs.event_id
        )
    )
    WHERE EXISTS (
        SELECT 1
        FROM zap_receipts extra
        WHERE extra.target_address <> ''
          AND extra.target_address = (rs.kind::text || ':' || lower(rs.pubkey) || ':' || rs.d_tag)
          AND extra.event_id IS DISTINCT FROM rs.event_id
    )
    GROUP BY rs.event_id
) AS computed
WHERE stats.event_id = computed.event_id
  AND stats.zap_msats IS DISTINCT FROM computed.zap_msats;

-- Same windows as RefreshPaidRankings, including the note's own created_at.
DELETE FROM note_zap_window_stats;

INSERT INTO note_zap_window_stats (
    event_id, author_pubkey, kind,
    sats_24h, zap_count_24h,
    sats_7d, zap_count_7d,
    sats_30d, zap_count_30d,
    projected_at
)
SELECT z.event_id,
       e.pubkey,
       e.kind,
       COALESCE(SUM(z.amount_sats) FILTER (
           WHERE z.created_at >= extract(epoch FROM now() - interval '24 hours')::bigint
             AND e.created_at >= extract(epoch FROM now() - interval '24 hours')::bigint
       ), 0)::bigint,
       COUNT(*) FILTER (
           WHERE z.created_at >= extract(epoch FROM now() - interval '24 hours')::bigint
             AND e.created_at >= extract(epoch FROM now() - interval '24 hours')::bigint
       ),
       COALESCE(SUM(z.amount_sats) FILTER (
           WHERE z.created_at >= extract(epoch FROM now() - interval '7 days')::bigint
             AND e.created_at >= extract(epoch FROM now() - interval '7 days')::bigint
       ), 0)::bigint,
       COUNT(*) FILTER (
           WHERE z.created_at >= extract(epoch FROM now() - interval '7 days')::bigint
             AND e.created_at >= extract(epoch FROM now() - interval '7 days')::bigint
       ),
       COALESCE(SUM(z.amount_sats), 0)::bigint,
       COUNT(*),
       now()
FROM zap_receipts z
JOIN events e ON e.id = z.event_id
WHERE z.created_at >= extract(epoch FROM now() - interval '30 days')::bigint
  AND e.created_at >= extract(epoch FROM now() - interval '30 days')::bigint
  AND z.amount_sats > 0
  AND z.event_id IS NOT NULL
  AND z.event_id <> ''
  AND z.sender_pubkey <> e.pubkey
  AND e.kind IN (1, 30023)
  AND NOT EXISTS (
      SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
  )
GROUP BY z.event_id, e.pubkey, e.kind
HAVING COALESCE(SUM(z.amount_sats), 0) > 0;

DELETE FROM profile_support_stats;

INSERT INTO profile_support_stats (
    pubkey,
    capped_sats_24h, raw_sats_24h, supporters_24h,
    capped_sats_7d, raw_sats_7d, supporters_7d,
    capped_sats_30d, raw_sats_30d, supporters_30d,
    projected_at
)
SELECT pubkey,
       COALESCE(SUM(LEAST(sats_24h, 100000::numeric)) FILTER (WHERE sats_24h > 0), 0)::bigint,
       COALESCE(SUM(sats_24h) FILTER (WHERE sats_24h > 0), 0)::bigint,
       COUNT(*) FILTER (WHERE sats_24h > 0),
       COALESCE(SUM(LEAST(sats_7d, 100000::numeric)) FILTER (WHERE sats_7d > 0), 0)::bigint,
       COALESCE(SUM(sats_7d) FILTER (WHERE sats_7d > 0), 0)::bigint,
       COUNT(*) FILTER (WHERE sats_7d > 0),
       COALESCE(SUM(LEAST(sats_30d, 100000::numeric)) FILTER (WHERE sats_30d > 0), 0)::bigint,
       COALESCE(SUM(sats_30d) FILTER (WHERE sats_30d > 0), 0)::bigint,
       COUNT(*) FILTER (WHERE sats_30d > 0),
       now()
FROM (
    SELECT receiver_pubkey AS pubkey,
           sender_pubkey,
           COALESCE(SUM(amount_sats) FILTER (
               WHERE created_at >= extract(epoch FROM now() - interval '24 hours')::bigint
           ), 0) AS sats_24h,
           COALESCE(SUM(amount_sats) FILTER (
               WHERE created_at >= extract(epoch FROM now() - interval '7 days')::bigint
           ), 0) AS sats_7d,
           COALESCE(SUM(amount_sats), 0) AS sats_30d
    FROM zap_receipts
    WHERE created_at >= extract(epoch FROM now() - interval '30 days')::bigint
      AND amount_sats > 0
      AND COALESCE(receiver_pubkey, '') <> ''
      AND sender_pubkey <> ''
      AND sender_pubkey <> receiver_pubkey
    GROUP BY receiver_pubkey, sender_pubkey
) AS sender_totals
GROUP BY pubkey
HAVING COALESCE(SUM(sats_30d), 0) > 0;

ANALYZE zap_receipts;
ANALYZE note_discovery_stats;
ANALYZE note_zap_window_stats;
ANALYZE profile_support_stats;
