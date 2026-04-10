CREATE TABLE IF NOT EXISTS search_documents (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL DEFAULT '',
    aliases TEXT[] NOT NULL DEFAULT '{}',
    identity_tokens TEXT[] NOT NULL DEFAULT '{}',
    freshness TIMESTAMPTZ NOT NULL DEFAULT now(),
    popularity DOUBLE PRECISION NOT NULL DEFAULT 0,
    trust_score DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'simple',
            coalesce(entity_id, '') || ' ' ||
            coalesce(title, '') || ' ' ||
            coalesce(body, '')
        )
    ) STORED,
    PRIMARY KEY (entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_search_documents_search_tsv
    ON search_documents USING gin (search_tsv);

CREATE INDEX IF NOT EXISTS idx_search_documents_type_popularity
    ON search_documents (entity_type, popularity DESC, freshness DESC);

CREATE INDEX IF NOT EXISTS idx_search_documents_freshness
    ON search_documents (freshness DESC, updated_at DESC);

CREATE OR REPLACE FUNCTION sync_search_document_profile() RETURNS trigger AS $$
DECLARE
    identity_key TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM search_documents
        WHERE entity_type = 'profile' AND entity_id = OLD.pubkey;
        IF OLD.nip05 IS NOT NULL AND btrim(OLD.nip05) <> '' THEN
            identity_key := lower(btrim(OLD.nip05));
            DELETE FROM search_documents
            WHERE entity_type = 'identity' AND entity_id = identity_key;
            INSERT INTO search_documents (
                entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
            )
            SELECT
                'identity',
                identity_key,
                profile.nip05,
                concat_ws(' ', profile.display_name, profile.name, profile.about),
                array_remove(array[profile.nip05, profile.display_name, profile.name, profile.pubkey], NULL),
                array_remove(array[profile.nip05, profile.pubkey], NULL),
                now(),
                0,
                NULL,
                now()
            FROM profiles_latest profile
            WHERE lower(btrim(profile.nip05)) = identity_key
            ORDER BY profile.metadata_created_at DESC, profile.metadata_event_id DESC
            LIMIT 1
            ON CONFLICT (entity_type, entity_id) DO UPDATE
            SET title = EXCLUDED.title,
                body = EXCLUDED.body,
                aliases = EXCLUDED.aliases,
                identity_tokens = EXCLUDED.identity_tokens,
                freshness = EXCLUDED.freshness,
                popularity = EXCLUDED.popularity,
                trust_score = EXCLUDED.trust_score,
                updated_at = now();
        END IF;
        RETURN OLD;
    END IF;

    INSERT INTO search_documents (
        entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
    )
    VALUES (
        'profile',
        NEW.pubkey,
        coalesce(nullif(NEW.display_name, ''), nullif(NEW.name, ''), NEW.pubkey),
        coalesce(NEW.about, ''),
        array_remove(array[NEW.pubkey, NEW.name, NEW.display_name, NEW.nip05], NULL),
        array_remove(array[NEW.pubkey, NEW.nip05], NULL),
        now(),
        coalesce((SELECT (coalesce(stats.follower_count, 0) + coalesce(stats.note_count, 0))::double precision
                  FROM profile_public_stats stats
                  WHERE stats.pubkey = NEW.pubkey), 0),
        NULL,
        now()
    )
    ON CONFLICT (entity_type, entity_id) DO UPDATE
    SET title = EXCLUDED.title,
        body = EXCLUDED.body,
        aliases = EXCLUDED.aliases,
        identity_tokens = EXCLUDED.identity_tokens,
        freshness = EXCLUDED.freshness,
        popularity = EXCLUDED.popularity,
        trust_score = EXCLUDED.trust_score,
        updated_at = now();

    IF NEW.nip05 IS NOT NULL AND btrim(NEW.nip05) <> '' THEN
        identity_key := lower(btrim(NEW.nip05));
        INSERT INTO search_documents (
            entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
        )
        VALUES (
            'identity',
            identity_key,
            NEW.nip05,
            concat_ws(' ', NEW.display_name, NEW.name, NEW.about),
            array_remove(array[NEW.nip05, NEW.display_name, NEW.name, NEW.pubkey], NULL),
            array_remove(array[NEW.nip05, NEW.pubkey], NULL),
            now(),
            0,
            NULL,
            now()
        )
        ON CONFLICT (entity_type, entity_id) DO UPDATE
        SET title = EXCLUDED.title,
            body = EXCLUDED.body,
            aliases = EXCLUDED.aliases,
            identity_tokens = EXCLUDED.identity_tokens,
            freshness = EXCLUDED.freshness,
            popularity = EXCLUDED.popularity,
            trust_score = EXCLUDED.trust_score,
            updated_at = now();
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_search_document_note() RETURNS trigger AS $$
BEGIN
    IF coalesce(NEW.kind, 0) <> 1 THEN
        RETURN NEW;
    END IF;
    INSERT INTO search_documents (
        entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
    )
    VALUES (
        'note',
        NEW.id,
        left(regexp_replace(coalesce(NEW.content, ''), '\s+', ' ', 'g'), 160),
        coalesce(NEW.content, ''),
        array_remove(array[NEW.id, NEW.pubkey], NULL),
        array_remove(array[NEW.pubkey], NULL),
        to_timestamp(NEW.created_at),
        coalesce((SELECT nds.score_7d::double precision FROM note_discovery_stats nds WHERE nds.event_id = NEW.id), 0),
        NULL,
        now()
    )
    ON CONFLICT (entity_type, entity_id) DO UPDATE
    SET title = EXCLUDED.title,
        body = EXCLUDED.body,
        aliases = EXCLUDED.aliases,
        identity_tokens = EXCLUDED.identity_tokens,
        freshness = EXCLUDED.freshness,
        popularity = EXCLUDED.popularity,
        trust_score = EXCLUDED.trust_score,
        updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_search_document_note() RETURNS trigger AS $$
BEGIN
    IF coalesce(OLD.kind, 0) = 1 THEN
        DELETE FROM search_documents
        WHERE entity_type = 'note' AND entity_id = OLD.id;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_search_document_hashtag(tag TEXT) RETURNS void AS $$
BEGIN
    IF tag IS NULL OR btrim(tag) = '' THEN
        RETURN;
    END IF;
    DELETE FROM search_documents
    WHERE entity_type = 'hashtag'
      AND entity_id = lower(tag)
      AND NOT EXISTS (SELECT 1 FROM event_hashtags WHERE hashtag = lower(tag));

    INSERT INTO search_documents (
        entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
    )
    SELECT
        'hashtag',
        lower(eh.hashtag),
        '#' || lower(eh.hashtag),
        '',
        array_remove(array[lower(eh.hashtag), '#' || lower(eh.hashtag)], NULL),
        '{}'::text[],
        coalesce(max(to_timestamp(eh.created_at)), now()),
        count(*)::double precision,
        NULL,
        now()
    FROM event_hashtags eh
    WHERE eh.hashtag = lower(tag)
    GROUP BY lower(eh.hashtag)
    ON CONFLICT (entity_type, entity_id) DO UPDATE
    SET title = EXCLUDED.title,
        body = EXCLUDED.body,
        aliases = EXCLUDED.aliases,
        identity_tokens = EXCLUDED.identity_tokens,
        freshness = EXCLUDED.freshness,
        popularity = EXCLUDED.popularity,
        trust_score = EXCLUDED.trust_score,
        updated_at = now();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_search_document_hashtag_insert() RETURNS trigger AS $$
BEGIN
    PERFORM refresh_search_document_hashtag(NEW.hashtag);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_search_document_hashtag_delete() RETURNS trigger AS $$
BEGIN
    PERFORM refresh_search_document_hashtag(OLD.hashtag);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_search_document_relay(relay TEXT) RETURNS void AS $$
BEGIN
    IF relay IS NULL OR btrim(relay) = '' THEN
        RETURN;
    END IF;
    DELETE FROM search_documents
    WHERE entity_type = 'relay'
      AND entity_id = relay
      AND NOT EXISTS (SELECT 1 FROM ingest_checkpoints WHERE relay_url = relay);

    INSERT INTO search_documents (
        entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
    )
    SELECT
        'relay',
        ic.relay_url,
        ic.relay_url,
        '',
        array_remove(array[ic.relay_url], NULL),
        '{}'::text[],
        coalesce(max(ic.updated_at), now()),
        count(*)::double precision,
        NULL,
        now()
    FROM ingest_checkpoints ic
    WHERE ic.relay_url = relay
    GROUP BY ic.relay_url
    ON CONFLICT (entity_type, entity_id) DO UPDATE
    SET title = EXCLUDED.title,
        body = EXCLUDED.body,
        aliases = EXCLUDED.aliases,
        identity_tokens = EXCLUDED.identity_tokens,
        freshness = EXCLUDED.freshness,
        popularity = EXCLUDED.popularity,
        trust_score = EXCLUDED.trust_score,
        updated_at = now();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_search_document_relay_upsert() RETURNS trigger AS $$
BEGIN
    PERFORM refresh_search_document_relay(NEW.relay_url);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_search_document_relay_delete() RETURNS trigger AS $$
BEGIN
    PERFORM refresh_search_document_relay(OLD.relay_url);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_search_document_profile_sync ON profiles_latest;
CREATE TRIGGER trg_search_document_profile_sync
AFTER INSERT OR UPDATE OR DELETE ON profiles_latest
FOR EACH ROW
EXECUTE FUNCTION sync_search_document_profile();

DROP TRIGGER IF EXISTS trg_search_document_note_sync ON events;
CREATE TRIGGER trg_search_document_note_sync
AFTER INSERT OR UPDATE ON events
FOR EACH ROW
EXECUTE FUNCTION sync_search_document_note();

DROP TRIGGER IF EXISTS trg_search_document_note_delete ON events;
CREATE TRIGGER trg_search_document_note_delete
AFTER DELETE ON events
FOR EACH ROW
EXECUTE FUNCTION delete_search_document_note();

DROP TRIGGER IF EXISTS trg_search_document_hashtag_insert ON event_hashtags;
CREATE TRIGGER trg_search_document_hashtag_insert
AFTER INSERT ON event_hashtags
FOR EACH ROW
EXECUTE FUNCTION sync_search_document_hashtag_insert();

DROP TRIGGER IF EXISTS trg_search_document_hashtag_delete ON event_hashtags;
CREATE TRIGGER trg_search_document_hashtag_delete
AFTER DELETE ON event_hashtags
FOR EACH ROW
EXECUTE FUNCTION sync_search_document_hashtag_delete();

DROP TRIGGER IF EXISTS trg_search_document_relay_upsert ON ingest_checkpoints;
CREATE TRIGGER trg_search_document_relay_upsert
AFTER INSERT OR UPDATE ON ingest_checkpoints
FOR EACH ROW
EXECUTE FUNCTION sync_search_document_relay_upsert();

DROP TRIGGER IF EXISTS trg_search_document_relay_delete ON ingest_checkpoints;
CREATE TRIGGER trg_search_document_relay_delete
AFTER DELETE ON ingest_checkpoints
FOR EACH ROW
EXECUTE FUNCTION sync_search_document_relay_delete();

INSERT INTO search_documents (
    entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
)
SELECT
    'profile',
    profile.pubkey,
    coalesce(nullif(profile.display_name, ''), nullif(profile.name, ''), profile.pubkey),
    coalesce(profile.about, ''),
    array_remove(array[profile.pubkey, profile.name, profile.display_name, profile.nip05], NULL),
    array_remove(array[profile.pubkey, profile.nip05], NULL),
    now(),
    coalesce((SELECT (coalesce(stats.follower_count, 0) + coalesce(stats.note_count, 0))::double precision
              FROM profile_public_stats stats
              WHERE stats.pubkey = profile.pubkey), 0),
    NULL,
    now()
FROM profiles_latest profile
ON CONFLICT (entity_type, entity_id) DO UPDATE
SET title = EXCLUDED.title,
    body = EXCLUDED.body,
    aliases = EXCLUDED.aliases,
    identity_tokens = EXCLUDED.identity_tokens,
    freshness = EXCLUDED.freshness,
    popularity = EXCLUDED.popularity,
    trust_score = EXCLUDED.trust_score,
    updated_at = now();

INSERT INTO search_documents (
    entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
)
SELECT
    'identity',
    lower(profile.nip05),
    profile.nip05,
    concat_ws(' ', profile.display_name, profile.name, profile.about),
    array_remove(array[profile.nip05, profile.display_name, profile.name, profile.pubkey], NULL),
    array_remove(array[profile.nip05, profile.pubkey], NULL),
    now(),
    0,
    NULL,
    now()
FROM (
    SELECT DISTINCT ON (lower(nip05))
        pubkey, nip05, name, display_name, about, metadata_created_at, metadata_event_id
    FROM profiles_latest
    WHERE nip05 IS NOT NULL AND btrim(nip05) <> ''
    ORDER BY lower(nip05), metadata_created_at DESC, metadata_event_id DESC
) profile
ON CONFLICT (entity_type, entity_id) DO UPDATE
SET title = EXCLUDED.title,
    body = EXCLUDED.body,
    aliases = EXCLUDED.aliases,
    identity_tokens = EXCLUDED.identity_tokens,
    freshness = EXCLUDED.freshness,
    popularity = EXCLUDED.popularity,
    trust_score = EXCLUDED.trust_score,
    updated_at = now();

INSERT INTO search_documents (
    entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
)
SELECT
    'note',
    events.id,
    left(regexp_replace(coalesce(events.content, ''), '\s+', ' ', 'g'), 160),
    coalesce(events.content, ''),
    array_remove(array[events.id, events.pubkey], NULL),
    array_remove(array[events.pubkey], NULL),
    to_timestamp(events.created_at),
    coalesce(nds.score_7d::double precision, 0),
    NULL,
    now()
FROM events
LEFT JOIN note_discovery_stats nds ON nds.event_id = events.id
WHERE events.kind = 1
ON CONFLICT (entity_type, entity_id) DO UPDATE
SET title = EXCLUDED.title,
    body = EXCLUDED.body,
    aliases = EXCLUDED.aliases,
    identity_tokens = EXCLUDED.identity_tokens,
    freshness = EXCLUDED.freshness,
    popularity = EXCLUDED.popularity,
    trust_score = EXCLUDED.trust_score,
    updated_at = now();

INSERT INTO search_documents (
    entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
)
SELECT
    'hashtag',
    lower(eh.hashtag),
    '#' || lower(eh.hashtag),
    '',
    array_remove(array[lower(eh.hashtag), '#' || lower(eh.hashtag)], NULL),
    '{}'::text[],
    coalesce(max(to_timestamp(eh.created_at)), now()),
    count(*)::double precision,
    NULL,
    now()
FROM event_hashtags eh
GROUP BY lower(eh.hashtag)
ON CONFLICT (entity_type, entity_id) DO UPDATE
SET title = EXCLUDED.title,
    body = EXCLUDED.body,
    aliases = EXCLUDED.aliases,
    identity_tokens = EXCLUDED.identity_tokens,
    freshness = EXCLUDED.freshness,
    popularity = EXCLUDED.popularity,
    trust_score = EXCLUDED.trust_score,
    updated_at = now();

INSERT INTO search_documents (
    entity_type, entity_id, title, body, aliases, identity_tokens, freshness, popularity, trust_score, updated_at
)
SELECT
    'relay',
    ic.relay_url,
    ic.relay_url,
    '',
    array_remove(array[ic.relay_url], NULL),
    '{}'::text[],
    max(ic.updated_at),
    count(*)::double precision,
    NULL,
    now()
FROM ingest_checkpoints ic
GROUP BY ic.relay_url
ON CONFLICT (entity_type, entity_id) DO UPDATE
SET title = EXCLUDED.title,
    body = EXCLUDED.body,
    aliases = EXCLUDED.aliases,
    identity_tokens = EXCLUDED.identity_tokens,
    freshness = EXCLUDED.freshness,
    popularity = EXCLUDED.popularity,
    trust_score = EXCLUDED.trust_score,
    updated_at = now();
