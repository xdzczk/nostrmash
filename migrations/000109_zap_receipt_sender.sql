-- Kind 9735 is signed by the lightning service. The person who paid is
-- the zap-request pubkey inside the description tag, then the NIP-57 P
-- tag. sender_pubkey stored the service, so a profile sent-zap page,
-- which filters on that column, was empty for every person.
--
-- Production statement_timeout is 15s. Parsing receipt JSON does not
-- finish in that window. This setting lasts only for this transaction.
SET LOCAL statement_timeout = 0;

CREATE OR REPLACE FUNCTION migration_000109_zap_receipt_sender(raw jsonb)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    tag jsonb;
    request jsonb;
    candidate text;
BEGIN
    IF raw IS NULL OR jsonb_typeof(raw->'tags') <> 'array' THEN
        RETURN NULL;
    END IF;

    FOR tag IN
        SELECT value FROM jsonb_array_elements(raw->'tags')
    LOOP
        IF jsonb_typeof(tag) <> 'array' THEN
            CONTINUE;
        END IF;
        IF jsonb_array_length(tag) < 2 OR tag->>0 <> 'description' THEN
            CONTINUE;
        END IF;
        BEGIN
            request := (tag->>1)::jsonb;
        EXCEPTION
            WHEN others THEN
                request := NULL;
        END;
        IF jsonb_typeof(request) = 'object' THEN
            candidate := request->>'pubkey';
            IF candidate ~ '^[0-9a-fA-F]{64}$' THEN
                RETURN lower(candidate);
            END IF;
        END IF;
    END LOOP;

    FOR tag IN
        SELECT value FROM jsonb_array_elements(raw->'tags')
    LOOP
        IF jsonb_typeof(tag) <> 'array' THEN
            CONTINUE;
        END IF;
        IF jsonb_array_length(tag) >= 2
           AND tag->>0 = 'P'
           AND tag->>1 ~ '^[0-9a-fA-F]{64}$' THEN
            RETURN lower(tag->>1);
        END IF;
    END LOOP;

    RETURN NULL;
END;
$$;

UPDATE zap_receipts AS z
SET sender_pubkey = parsed.sender
FROM (
    SELECT e.id AS zap_receipt_id, migration_000109_zap_receipt_sender(e.raw_json) AS sender
    FROM events e
    WHERE e.kind = 9735
) AS parsed
WHERE z.zap_receipt_id = parsed.zap_receipt_id
  AND parsed.sender IS NOT NULL
  AND z.sender_pubkey IS DISTINCT FROM parsed.sender;

-- content_zap_count is receipts this pubkey sent. Move it off the
-- lightning services and onto the payers named above.
UPDATE profile_public_stats AS pps
SET content_zap_count = zs.cnt
FROM (
    SELECT sender_pubkey AS pubkey, COUNT(*)::bigint AS cnt
    FROM zap_receipts
    GROUP BY sender_pubkey
) AS zs
WHERE pps.pubkey = zs.pubkey
  AND pps.content_zap_count IS DISTINCT FROM zs.cnt;

UPDATE profile_public_stats AS pps
SET content_zap_count = 0
WHERE pps.content_zap_count <> 0
  AND NOT EXISTS (
      SELECT 1
      FROM zap_receipts z
      WHERE z.sender_pubkey = pps.pubkey
  );

DROP FUNCTION migration_000109_zap_receipt_sender(jsonb);

ANALYZE zap_receipts;
